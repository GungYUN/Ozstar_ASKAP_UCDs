"""Offline regression tests: real SQLite/CSV/tar/FITS, simulated CASDA/Slurm/DStools.

Run: python -m unittest discover -s tests -v (from ozstar_askap).
No server access, credentials, external MS, or container is required.
"""
import ast
import copy
import logging
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
from collections import Counter
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import astropy.units as u
import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.table import Table, vstack
from astropy.wcs import WCS

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config
import compare_crosscatalogues as comparison
import crosscatalogue as cross
import shared_controller as controller
import shared_worker as worker
import process_job as pipeline
import ozstar_main as main
import pipeline_utils as util
import sb_status as status
import gate9_retry_validation as gate9


def rows(sb=35429, beam=12):
    return Table({
        "obs_id": [f"ASKAP-{sb}"],
        "filename": [f"scienceData.RACS_1212+14.SB{sb}.beam{beam}_averaged.ms.tar"],
        "obs_publisher_did": [f"ivo://test/{sb}/{beam}"],
        "em_min": [0.21], "em_max": [0.3], "s_ra": [180.0], "s_dec": [0.0],
        "t_min": [59000.0], "access_estsize": [1],
    })


class SharedTests(unittest.TestCase):
    def setUp(self):
        old_disable = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, old_disable)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.root = root
        values = dict(WORK=root, STAGING_ROOT=root/"staging", STATE_ROOT=root/"state",
            PRODUCT_ROOT=root/"products", LOG_ROOT=root/"logs", WSCLEAN_TEMP_ROOT=root/"staging/_wsclean_tmp",
            CATALOGUE_PATH=root/"catalogue.csv", TIME_CSV_PATH=root/"epochs.csv", CONTAINER=root/"candidate.sif",
            CROSSCATALOGUE_QUERY_DELAY_SECONDS=0, CASDA_SOURCE_RETRY_DELAY_SECONDS=0,
            CASDA_SOURCE_DOWNLOAD_RETRIES=1, CPU_PER_SLOT=8, PARALLEL_SLOTS=1,
            CREATE_MODEL_THREADS=8, STAGING_BUDGET_BYTES=2**40,
            CASDA_RADIUS_FREQUENCY_MODE="top")
        for key, value in values.items():
            p = patch.object(config, key, value)
            p.start()
            self.addCleanup(p.stop)
        for module, key, value in ((util,"PRODUCT_ROOT",config.PRODUCT_ROOT),
                                   (util,"STATE_ROOT",config.STATE_ROOT),
                                   (status,"DATABASE_PATH",config.STATE_ROOT/"sb_processing.sqlite")):
            p = patch.object(module, key, value)
            p.start()
            self.addCleanup(p.stop)
        config.ensure_directories()
        config.CATALOGUE_PATH.write_text("catalogue fixture")
        config.CONTAINER.write_bytes(b"test SIF identity")
        self.table = rows()
        self.obs = cross.observation(self.table[0])
        self.calls = Counter()
        self.fail_insert = set()
        self.sources = [self.source(1), self.source(99)]
        self.group = dict(observation=self.obs, sources=self.sources,
                          crosscatalogue_revision="test-revision", seed_source="UCS1")
        self.settings = worker.model_settings(cross.digest_file(config.CONTAINER))

    def source(self, number):
        path = config.STATE_ROOT / f"UCS{number}.ecsv"
        self.table.write(path, format="ascii.ecsv", overwrite=True)
        return dict(source_name=f"UCS{number}", ucs_number=number, ra=180.0+(number != 1)*0.1,
            dec=0.0, pmra=0.0, pmdec=0.0, sptnum=25., batch=config.batch_name(number),
            query_cache=str(path), query_status="available", eligible=True,
            observations=[copy.deepcopy(self.obs)])

    def test_central_frequency_radius_and_invalid_bounds(self):
        c = cross.casda_query.SPEED_OF_LIGHT_METRES_PER_SECOND
        table = Table({"em_min": [c/1.6e9, c/1.6e9, -1.0],
                       "em_max": [c/1.0e9, c/1.0e9, 1.0]})
        separation = np.array([0.5, 0.58, 0.1])
        self.assertEqual(len(cross.casda_query._half_power_filter(table, separation)[0]), 1)
        with patch.object(config, "CASDA_RADIUS_FREQUENCY_MODE", "central"):
            self.assertEqual(len(cross.casda_query._half_power_filter(table, separation)[0]), 2)
            values = cross.casda_query.frequency_selection(c/1.6e9, c/1.0e9)
            self.assertAlmostEqual(values["radius_frequency_ghz"], 1.3)
            self.assertAlmostEqual(values["half_power_radius_deg"], 0.84/1.3)
        self.assertEqual(cross.casda_query.frequency_selection(0.21, 0.3)["radius_frequency_mode"], "top")

    def test_frequency_mode_cache_isolation_and_old_top_compatibility(self):
        table = self.table.copy()
        table.meta.update(cross.casda_query._cache_selection_metadata(180, 0))
        top_signature = cross.selection_signature()
        self.assertNotIn("askap_radius_frequency_mode", table.meta)
        self.assertTrue(cross.casda_query._cache_matches_current_selection(table, 180, 0)[0])
        with patch.object(config, "CASDA_RADIUS_FREQUENCY_MODE", "central"):
            self.assertNotEqual(top_signature, cross.selection_signature())
            self.assertFalse(cross.casda_query._cache_matches_current_selection(table, 180, 0)[0])
            table.meta.update(cross.casda_query._cache_selection_metadata(180, 0))
            self.assertTrue(cross.casda_query._cache_matches_current_selection(table, 180, 0)[0])
            self.assertEqual(cross.observation(table[0])["key"], self.obs["key"])
        self.assertFalse(cross.casda_query._cache_matches_current_selection(table, 180, 0)[0])

    def test_frequency_mode_retains_nearest_qualified_beam_rule(self):
        c = cross.casda_query.SPEED_OF_LIGHT_METRES_PER_SECOND
        table = vstack([rows(35429, 12), rows(35429, 13)])
        table["quality_level"] = ["GOOD", "GOOD"]
        table["s_ra"] = [180.58, 180.6]
        table["em_min"] = [c/1.6e9, c/1.0e9]
        table["em_max"] = [c/1.0e9, c/0.9e9]
        casda = SimpleNamespace(Casda=SimpleNamespace(filter_out_unreleased=lambda rows: rows))
        with patch.dict(sys.modules, {"astroquery.casda": casda}):
            selected = cross.casda_query._filter_and_deduplicate(table, 180, 0)
            self.assertEqual(cross.observation(selected[0])["beam"], "beam13")
            with patch.object(config, "CASDA_RADIUS_FREQUENCY_MODE", "central"):
                selected = cross.casda_query._filter_and_deduplicate(table, 180, 0)
                self.assertEqual(cross.observation(selected[0])["beam"], "beam12")

    def test_central_index_exports_actual_selection_and_top_default_unchanged(self):
        catalogue = pd.DataFrame([dict(UCS=1, UCS_name="UCS1")])
        with patch.object(config, "CASDA_RADIUS_FREQUENCY_MODE", "central"), \
                patch.object(cross.casda_query, "query_source_table", return_value=self.table):
            snapshot = cross.ensure_catalogue(catalogue, lambda row: self.sources[0], update=True)
        obs = snapshot["sources"][0]["observations"][0]
        self.assertEqual(obs["radius_frequency_mode"], "central")
        summary = util.read_json(config.STATE_ROOT/"crosscatalogue-summary.json", {})
        self.assertEqual(summary["radius_frequency_mode"], "central")
        # Exporting a frozen central snapshot while config is back to top must not relabel it.
        cross.export_tables(snapshot)
        exported = pd.read_csv(config.STATE_ROOT/"crosscatalogue_observations.csv")
        self.assertEqual(exported.iloc[0]["radius_frequency_mode"], "central")
        self.assertAlmostEqual(exported.iloc[0]["radius_frequency_ghz"], obs["frequency_central_ghz"])

    def test_frequency_comparison_distinguishes_replacements_and_failed_queries(self):
        signature = cross.selection_signature()
        top = dict(signature=signature, revision="top", sources=copy.deepcopy(self.sources))
        central = copy.deepcopy(top)
        central["revision"] = "central"
        central["signature"]["selection"]["askap_radius_frequency_mode"] = "central"
        central["sources"][0]["observations"] = [cross.observation(rows(35429, 13)[0])]
        central["sources"][1]["query_status"] = "query_failed"
        summary, source_rows, differences = comparison.compare(top, central)
        self.assertEqual(summary["comparable_sources"], 1)
        self.assertEqual(summary["uncomparable_sources"], 1)
        self.assertEqual(summary["replaced_sb_count"], 1)
        self.assertEqual({d["change_kind"] for d in differences}, {"beam_changed"})
        self.assertEqual(len(differences), 2)
        self.assertEqual(source_rows[1]["comparison_status"], "uncomparable_query")
        central["signature"]["query_limit"] += 1
        with self.assertRaisesRegex(ValueError, "settings differ"):
            comparison.compare(top, central)

    def test_frequency_comparison_cli_writes_csv_without_changing_inputs(self):
        top = dict(signature=cross.selection_signature(), revision="top", finished_at="complete",
                   sources=copy.deepcopy(self.sources))
        central = copy.deepcopy(top)
        central["revision"] = "central"
        central["signature"]["selection"]["askap_radius_frequency_mode"] = "central"
        paths = [self.root/"top", self.root/"central"]
        for path, snapshot in zip(paths, (top, central)):
            util.atomic_write_json(path/"state/crosscatalogue.json", snapshot)
        hashes = [cross.digest_file(p/"state/crosscatalogue.json") for p in paths]
        output = self.root/"comparison"
        command = [sys.executable, str(Path(comparison.__file__)), "--top-work", str(paths[0]),
            "--central-work", str(paths[1]), "--output-dir", str(output)]
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = util.read_json(output/"comparison-summary.json", {})
        self.assertEqual(summary["changed_sources"], 0)
        self.assertEqual(summary["uncomparable_sources"], 0)
        self.assertEqual(len(pd.read_csv(output/"observation_differences.csv")), 0)
        self.assertEqual(len(pd.read_csv(output/"source_comparison.csv")), 2)
        self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
        self.assertEqual(hashes, [cross.digest_file(p/"state/crosscatalogue.json") for p in paths])
        with self.assertRaisesRegex(ValueError, "Expected central"):
            comparison.load_index(paths[0], "central")
        central.pop("finished_at")
        util.atomic_write_json(paths[1]/"state/crosscatalogue.json", central)
        with self.assertRaisesRegex(ValueError, "not finished"):
            comparison.load_index(paths[1], "central")

    def manifest(self, targets=None, retry=False):
        targets = self.sources if targets is None else targets
        for source in targets:
            status.initialize_source_rows(source, self.table)
        return controller.make_manifest(self.group, targets, self.settings, retry)

    def archive(self, manifest):
        archive = Path(manifest["archive_path"])
        archive.parent.mkdir(parents=True, exist_ok=True)
        obs = manifest.get("observation", self.obs)
        scratch = self.root / "archive-source" / obs["filename"][:-4]
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / "table.dat").write_text("untouched")
        with tarfile.open(archive, "w") as tar:
            tar.add(scratch, arcname=scratch.name)
        Path(str(archive)+".checksum").write_text("retained-checksum")

    def fake_dstools(self, command, cwd, log_path, deadline):
        exe = command[0]
        self.calls[exe] += 1
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a") as out:
            out.write(repr(command)+"\nstdout fixture\nstderr fixture\n")
        if exe == "dstools-askap-preprocess":
            # Keep the mock above SQLite's millisecond timing resolution.
            time.sleep(0.002)
            (Path(command[-1])/"FIELD_OLD").mkdir(exist_ok=True)
        elif exe == "dstools-create-model":
            model = cwd / "wsclean_model"
            model.mkdir(exist_ok=True)
            (model/"wsclean-MFS-I-model.fits").write_text("master")
            (model/"wsclean-0000-I-model.fits").write_text("unmasked")
            wcs = WCS(naxis=2)
            wcs.wcs.crpix = [32.,32.]
            wcs.wcs.cdelt = [-0.01,0.01]
            wcs.wcs.crval = [180.,0.]
            wcs.wcs.ctype = ["RA---TAN","DEC--TAN"]
            for pol in ("I","V"):
                fits.PrimaryHDU(np.ones((64,64)), wcs.to_header()).writeto(
                    model/f"wsclean-MFS-{pol}-image.fits", overwrite=True)
        elif exe == "dstools-insert-model":
            self.assertTrue((pipeline._find_ms(cwd) / "FIELD_OLD").is_dir(), "Insert must use preprocessed MS")
            channel = cwd / "wsclean_model/wsclean-0000-I-model.fits"
            self.assertEqual(channel.read_text(), "unmasked")
            channel.write_text("masked for " + str(cwd))
            source = cwd.parent.parent.name
            if source in self.fail_insert:
                raise pipeline.ProcessingError("injected insert failure")
        elif exe == "dstools-subtract-model":
            (cwd / "fixture.subtracted.ms").mkdir()
        elif exe == "dstools-extract-ds":
            Path(command[-1]).write_text("DS fixture")
        elif exe == "/opt/askap/bin/python":
            # Simulate reset_askap_ms only, not the scientific table conversion.
            old = Path(command[-1]) / "FIELD_OLD"
            if old.exists():
                old.rmdir()
        else:
            raise AssertionError(command)

    def run_worker(self, manifest):
        with patch.object(pipeline, "_container_command", side_effect=lambda *args:list(args)), \
             patch.object(pipeline, "_run_dstools", side_effect=self.fake_dstools):
            return worker.run_manifest(manifest)

    def test_product_identity_preserves_units(self):
        table = self.table.copy()
        table["em_min"] = [21.0] * u.cm
        table["em_max"] = [30.0] * u.cm
        self.assertEqual(cross.observation(table[0])["key"], self.obs["key"])
        self.assertNotEqual(cross.observation(rows(beam=13)[0])["key"], self.obs["key"])
        table["obs_publisher_did"][0] = "ivo://test/other"
        self.assertNotEqual(cross.observation(table[0])["key"], self.obs["key"])

    def hipass_table(self, field="00", beam="06"):
        table = self.table.copy()
        table["filename"] = [f"HIPASSJ0309-17_SB35429_F{field}_B{beam}.ms.tar"]
        return table

    def test_hipass_parser_identity_and_safe_basename(self):
        self.assertEqual(util.sb_beam_from_name(self.table["filename"][0]), ("SB35429", "beam12"))
        table = self.hipass_table()
        obs = cross.observation(table[0])
        self.assertEqual((obs["sb_id"], obs["beam"], obs["sb_name"]),
                         ("SB35429", "beam06", "SB35429_beam06"))
        self.assertNotEqual(obs["key"], cross.observation(self.hipass_table(field="01")[0])["key"])
        self.assertNotEqual(obs["key"], cross.observation(self.hipass_table(beam="07")[0])["key"])
        for filename in ("../HIPASS_SB35429_F00_B06.ms.tar",
                         "HIPASS_SB35429_B06.ms.tar", "HIPASS_SB35429_F00_B06x.ms.tar"):
            with self.subTest(filename=filename):
                table["filename"] = [filename]
                with self.assertRaises(ValueError):
                    cross.observation(table[0])
        table = self.hipass_table()
        table["obs_id"] = ["ASKAP-99999"]
        with self.assertRaisesRegex(ValueError, "mismatch"):
            cross.observation(table[0])

    def test_hipass_status_and_command_line_extraction(self):
        table = self.hipass_table()
        source = self.sources[0]
        status.initialize_source_rows(source, table)
        self.assertEqual(status.observation_status("UCS1", "SB35429", "beam06")["casda_query"], "Y")
        longobs = config.STAGING_ROOT / "UCS1/LongObs"
        longobs.mkdir(parents=True)
        ms_name = str(table["filename"][0])[:-4]
        raw = self.root / ms_name
        raw.mkdir()
        (raw / "table.dat").write_text("fixture")
        archive = longobs / str(table["filename"][0])
        with tarfile.open(archive, "w") as handle:
            handle.add(raw, arcname=ms_name)
        checksum = Path(str(archive) + ".checksum")
        checksum.write_text("retained")
        pipeline.decompress_and_move(longobs)
        self.assertTrue((longobs / "SB35429_beam06" / ms_name / "table.dat").is_file())
        self.assertFalse(archive.exists())
        self.assertTrue(checksum.is_file())
        self.assertEqual(status.observation_status("UCS1", "SB35429", "beam06")["extract_archive"], "Y")

    def test_hipass_shared_master_and_two_target_products(self):
        self.table = self.hipass_table()
        self.obs = cross.observation(self.table[0])
        self.sources = [self.source(1), self.source(99)]
        self.group = dict(observation=self.obs, sources=self.sources,
                          crosscatalogue_revision="hipass-test", seed_source="UCS1")
        manifest = self.manifest()
        self.archive(manifest)
        self.assertEqual(self.run_worker(manifest), 0)
        self.assertEqual(self.calls["dstools-create-model"], 1)
        for source in self.sources:
            self.assertTrue(worker.already_complete(source, self.obs))
            row = status.observation_status(source["source_name"], "SB35429", "beam06")
            self.assertEqual(row["retry_stage"], "complete")

    def test_hipass_redownload_cleanup_retains_other_beam(self):
        longobs = config.STAGING_ROOT / "UCS1/LongObs"
        sb_dir = longobs / "SB35429_beam06"
        sb_dir.mkdir(parents=True)
        target = longobs / "HIPASSJ0309-17_SB35429_F00_B06.ms.tar.checksum"
        other = longobs / "HIPASSJ0309-17_SB35429_F00_B07.ms.tar.checksum"
        target.write_text("failed input")
        other.write_text("other beam")
        pipeline._discard_sb_for_redownload("UCS1", sb_dir,
            config.PRODUCT_ROOT / "test-product", config.WSCLEAN_TEMP_ROOT / "test-temp")
        self.assertFalse(target.exists())
        self.assertTrue(other.exists())

    def test_range_expands_companions_not_their_other_observations(self):
        other = cross.observation(rows(sb=88888)[0])
        self.sources[1]["observations"].append(other)
        third = self.source(150)
        third["observations"] = [other]
        snapshot = dict(revision="x", sources=self.sources+[third])
        groups = cross.groups_for_range(snapshot, 1, 1)
        self.assertEqual(len(groups), 1)
        self.assertEqual([s["source_name"] for s in groups[0]["sources"]], ["UCS1","UCS99"])

    def test_single_model_two_independent_targets_and_crop(self):
        manifest = self.manifest()
        self.archive(manifest)
        self.assertEqual(self.run_worker(manifest), 0)
        self.assertEqual(self.calls["dstools-create-model"], 1)
        self.assertEqual(self.calls["dstools-askap-preprocess"], 1)
        self.assertEqual(self.calls["dstools-insert-model"], 2)
        headers = []
        for source in self.sources:
            self.assertTrue(worker.already_complete(source,self.obs))
            directory = worker.product_dir(source,self.obs)
            headers.append(fits.getheader(directory/"wsclean_model/wsclean-MFS-I-image.fits"))
            row = status.observation_status(source["source_name"],self.obs["sb_id"],self.obs["beam"])
            self.assertEqual(row["retry_stage"], "complete")
            self.assertEqual(row["model_reused"], 1)
        self.assertNotEqual(headers[0]["CRPIX1"], headers[1]["CRPIX1"])
        self.assertFalse((Path(manifest["shared_root"])/"master").exists())
        self.assertFalse(Path(manifest["archive_path"]).exists())
        self.assertTrue(Path(manifest["archive_path"]+".checksum").exists())

    def test_failure_retains_master_and_other_target_succeeds(self):
        manifest = self.manifest()
        self.archive(manifest)
        self.fail_insert = {"UCS1"}
        self.assertEqual(self.run_worker(manifest), 0)
        self.assertFalse(worker.already_complete(self.sources[0],self.obs))
        self.assertTrue(worker.already_complete(self.sources[1],self.obs))
        master = Path(manifest["shared_root"])/"master"
        self.assertTrue(master.is_dir())
        self.assertEqual((master/"wsclean_model/wsclean-0000-I-model.fits").read_text(), "unmasked")
        self.assertEqual(controller.select_targets(self.group, False), [])
        self.assertEqual(controller.select_targets(self.group, True), [self.sources[0]])
        row = status.observation_status("UCS1",self.obs["sb_id"],self.obs["beam"])
        self.assertEqual(row["error_step"], "insert_model")
        self.assertTrue(Path(row["failure_log_path"]).is_file())

    def test_explicit_retry_reset_then_one_redownload_then_exhausted(self):
        initial = self.manifest()
        self.archive(initial)
        self.fail_insert = {"UCS1"}
        self.run_worker(initial)
        retry = self.manifest([self.sources[0]], retry=True)
        controller.prepare(retry)  # Existing private MS, no CASDA.
        self.run_worker(retry)
        self.assertEqual(self.calls["/opt/askap/bin/python"], 1)
        self.assertEqual(self.calls["dstools-create-model"], 2)
        row = status.observation_status("UCS1",self.obs["sb_id"],self.obs["beam"])
        self.assertEqual(row["retry_stage"], "redownload_required")
        self.assertEqual(row["processing_retry_count"], 1)
        self.assertTrue((Path(initial["shared_root"])/"master").is_dir())
        final = self.manifest([self.sources[0]], retry=True)
        with patch.object(controller, "_download", side_effect=self.archive) as download:
            controller.prepare(final)
        self.assertEqual(download.call_count, 1)
        self.run_worker(final)
        row = status.observation_status("UCS1",self.obs["sb_id"],self.obs["beam"])
        self.assertEqual(row["retry_stage"], "exhausted")
        self.assertEqual(row["redownload_retry_count"], 1)
        self.assertEqual(controller.select_targets(self.group, True), [])

    def test_master_failure_first_retry_uses_retained_raw(self):
        manifest = self.manifest()
        self.archive(manifest)
        log = config.LOG_ROOT/"extraction.log"
        raw = worker.extract_archive(manifest,log)
        worker._record_failure(self.sources[0],self.obs,"create_model","fixture")
        retry = self.manifest([self.sources[0]], retry=True)
        with patch.object(controller, "_download") as download:
            controller.prepare(retry)
        download.assert_not_called()
        self.assertEqual(retry["fallback_raw_ms"], str(raw))

    def test_full_catalogue_query_status_update_and_cached_reuse(self):
        catalogue = pd.DataFrame({"UCS_name":["UCS1","UCS2","UCS3"], "UCS":[1,2,3]})
        source_from = lambda row: self.catalogue_source(int(row["UCS"]))
        def query(ra, dec):
            if ra == 3:
                raise RuntimeError("TAP temporarily down")
            return self.table.copy() if ra == 1 else self.table[:0]
        with patch.object(cross.casda_query,"query_source_table",side_effect=query) as mock:
            snapshot = cross.ensure_catalogue(catalogue, source_from)
            cross.ensure_catalogue(catalogue, source_from)
        self.assertEqual(mock.call_count, 3)
        self.assertEqual([s["query_status"] for s in snapshot["sources"]],
                         ["available","no_observations","query_failed"])
        with patch.object(cross.casda_query,"query_source_table",side_effect=RuntimeError("offline")):
            new = cross.ensure_catalogue(catalogue, source_from, update=True)
        self.assertNotEqual(new["revision"],snapshot["revision"])
        self.assertEqual(new["sources"][0]["observations"], snapshot["sources"][0]["observations"])
        self.assertTrue(new["sources"][0]["using_previous_result"])
        self.assertEqual(cross.groups_for_range(new,1,3), [])
        csv = Table.read(config.STATE_ROOT/"crosscatalogue_sources.csv",format="ascii.csv")
        self.assertEqual(len(csv), 3)
        self.assertEqual(set(csv["query_status"]), {"query_failed"})

    def test_interrupted_index_marks_unknown_and_resumes(self):
        catalogue = pd.DataFrame({"UCS_name":["UCS1","UCS2"],"UCS":[1,2]})
        source_from = lambda row: self.catalogue_source(int(row["UCS"]))
        with patch.object(cross.casda_query,"query_source_table",side_effect=[self.table,KeyboardInterrupt()]):
            with self.assertRaises(KeyboardInterrupt):
                cross.ensure_catalogue(catalogue,source_from)
        summary = util.read_json(config.STATE_ROOT/"crosscatalogue-summary.json")
        self.assertEqual(summary["unknown"],1)
        with patch.object(cross.casda_query,"query_source_table",return_value=self.table) as query:
            snapshot = cross.ensure_catalogue(catalogue,source_from)
        self.assertEqual(query.call_count,1)
        self.assertTrue(snapshot["finished_at"])

    def catalogue_source(self, number):
        return {key:value for key,value in dict(self.source(number),ra=float(number)).items()
                if key in {"source_name","ucs_number","ra","dec","pmra","pmdec","sptnum","batch"}}

    def failed_index_fixture(self):
        catalogue = pd.DataFrame({"UCS_name":["UCS1","UCS2","UCS3"], "UCS":[1,2,3]})
        source_from = lambda row: self.catalogue_source(int(row["UCS"]))
        def query(ra, dec):
            if ra != 1:
                raise OSError(122, "Disk quota exceeded")
            return self.table.copy()
        with patch.object(cross.casda_query, "query_source_table", side_effect=query):
            initial = cross.ensure_catalogue(catalogue, source_from)
        return catalogue, source_from, initial

    def test_retry_failed_index_keeps_success_rows_and_queries_only_failures(self):
        catalogue, source_from, initial = self.failed_index_fixture()
        good = initial["sources"][0]
        contents = Path(good["query_cache"]).read_bytes()
        with patch.object(cross.casda_query, "query_source_table", return_value=self.hipass_table()) as query:
            final = cross.ensure_catalogue(catalogue, source_from, retry_failed=True)
        self.assertEqual(query.call_count, 2)
        self.assertEqual([call.args[0] for call in query.call_args_list], [2.0, 3.0])
        self.assertEqual(final["sources"][0], good)
        self.assertEqual(Path(good["query_cache"]).read_bytes(), contents)
        self.assertNotEqual(initial["revision"], final["revision"])
        self.assertEqual([source["query_status"] for source in final["sources"]], ["available"] * 3)
        summary = util.read_json(config.STATE_ROOT / "crosscatalogue-summary.json")
        self.assertEqual((summary["unknown"], summary["query_failed"]), (0, 0))

    def test_retry_failed_index_resumes_interruption_without_requerying_success(self):
        catalogue, source_from, initial = self.failed_index_fixture()
        with patch.object(cross.casda_query, "query_source_table", side_effect=[self.table, KeyboardInterrupt()]):
            with self.assertRaises(KeyboardInterrupt):
                cross.ensure_catalogue(catalogue, source_from, retry_failed=True)
        revision = util.read_json(config.STATE_ROOT / "crosscatalogue-update.json")["revision"]
        with patch.object(cross.casda_query, "query_source_table", return_value=self.table) as query:
            final = cross.ensure_catalogue(catalogue, source_from, retry_failed=True)
        self.assertEqual(query.call_count, 1)
        self.assertEqual(query.call_args.args[0], 3.0)
        self.assertEqual(final["revision"], revision)
        self.assertEqual(final["sources"][0], initial["sources"][0])

    def test_retry_failed_index_recovers_unpublished_quota_checkpoints(self):
        catalogue = pd.DataFrame({"UCS_name":["UCS1","UCS2"], "UCS":[1,2]})
        source_from = lambda row: self.catalogue_source(int(row["UCS"]))
        write = cross.atomic_write_json
        def quota_at_publication(path, value):
            if path.name == "crosscatalogue.json":
                raise OSError(122, "Disk quota exceeded")
            write(path, value)
        with patch.object(cross.casda_query, "query_source_table", side_effect=[self.table, OSError(122, "quota")]), \
             patch.object(cross, "atomic_write_json", side_effect=quota_at_publication):
            with self.assertRaises(OSError):
                cross.ensure_catalogue(catalogue, source_from)
        self.assertFalse((config.STATE_ROOT / "crosscatalogue.json").exists())
        # Simulate SIGKILL leaving the main snapshot stale while journals have
        # all completed attempts, as seen in the actual Ozstar output.
        draft = util.read_json(config.STATE_ROOT / "crosscatalogue-update.json")
        for source in draft["sources"]:
            source.update(query_done=False, query_status="unknown")
        util.atomic_write_json(config.STATE_ROOT / "crosscatalogue-update.json", draft)
        with patch.object(cross.casda_query, "query_source_table", return_value=self.hipass_table()) as query:
            final = cross.ensure_catalogue(catalogue, source_from, retry_failed=True)
        self.assertEqual(query.call_count, 1)
        self.assertEqual(query.call_args.args[0], 2.0)
        self.assertTrue(final["finished_at"])
        self.assertEqual(util.read_json(config.STATE_ROOT / "crosscatalogue-summary.json")["available"], 2)

    def test_retry_failed_index_guards_missing_or_changed_selection(self):
        catalogue = pd.DataFrame({"UCS_name":["UCS1"], "UCS":[1]})
        source_from = lambda row: self.catalogue_source(int(row["UCS"]))
        with self.assertRaisesRegex(ValueError, "build the index first"):
            cross.ensure_catalogue(catalogue, source_from, retry_failed=True)
        self.failed_index_fixture()
        with patch.object(config, "SPTNUM_THRESHOLD", 21):
            with self.assertRaisesRegex(ValueError, "selection changed"):
                cross.ensure_catalogue(catalogue, source_from, retry_failed=True)

    def test_retry_failed_index_cli_requires_index_only(self):
        for flags in (["--retry-crosscatalogue-failures"],
                      ["--retry-crosscatalogue-failures", "--crosscatalogue-only", "--update-crosscatalogue"]):
            result = subprocess.run([sys.executable, str(config.PROGRAM_DIR / "ozstar_main.py"), *flags],
                capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires --crosscatalogue-only", result.stderr)

    def test_bash_syntax_and_legacy_default_worker(self):
        manifest = self.manifest()
        path = Path(manifest["manifest_path"])
        for shared in (True,False):
            script = main._write_sbatch(path,"test-shared" if shared else "test-legacy","test",
                worker_script="shared_worker.py" if shared else "process_job.py",
                cpus=8 if shared else None, walltime_hours=2.0 if shared else None)
            subprocess.run(["bash","-n",str(script)],check=True)
            text = script.read_text()
            if shared:
                self.assertIn("#SBATCH --cpus-per-task=8",text)
                self.assertIn("#SBATCH --time=2:00:00",text)
                self.assertNotIn('if sbatch ',text)
                self.assertNotIn('heartbeat', text)
                self.assertNotIn('RESUBMIT_PENDING', text)
                self.assertNotIn('AUTO_RESUBMIT_COUNT', text)
                self.assertIn("export ASKAP_PARALLEL_SLOTS=1",text)
            else:
                self.assertIn('if sbatch ',text)
                self.assertIn('heartbeat_loop', text)

    def test_worker_killed_recorded_not_success(self):
        manifest = self.manifest()
        controller._fail_worker(manifest,"Slurm TIMEOUT")
        for source in self.sources:
            row = status.observation_status(source["source_name"],self.obs["sb_id"],self.obs["beam"])
            self.assertEqual(row["error_step"],"worker_setup")
            self.assertEqual(row["retry_stage"],"processing_failed")
        self.assertEqual(controller.select_targets(self.group,False), [])

    def test_final_download_failure_is_exhausted(self):
        self.manifest([self.sources[0]])
        status.set_retry_stage("UCS1",self.obs["sb_id"],"redownload_required",beam=self.obs["beam"])
        worker._record_failure(self.sources[0],self.obs,"archive_download","network failed")
        self.assertEqual(status.retry_stage("UCS1",self.obs["sb_id"],beam=self.obs["beam"]),"exhausted")

    def test_download_full_url_list_and_encoded_names(self):
        manifest = self.manifest()
        name = self.obs["filename"]
        urls = ["https://example.invalid/"+name.replace("+","%2B"),
                "https://example.invalid/"+name+".checksum"]
        client = SimpleNamespace(stage_data=lambda table: urls)
        received = []
        def download_files(url_list,savedir):
            received.append(url_list)
            self.archive(manifest)
        client.download_files = download_files
        with patch.object(controller.casda_query,"build_casda_client",return_value=client):
            controller._download(manifest)
        self.assertEqual(received,[urls])
        self.assertEqual(manifest["prepared_ms_units"],1)

    def test_controller_fast_worker_results_not_overwritten(self):
        args = SimpleNamespace(begin=1,end=1,retry_existing=False,update_crosscatalogue=False,
            refresh_cache=False,crosscatalogue_only=False,submit=True,prepare_download=False,
            max_resubmits=1,controller_poll_interval=1)
        snapshot = dict(revision="test-revision",sources=self.sources)
        def submit(script):
            path = script.with_suffix(".json")
            manifest = util.read_json(path)
            self.run_worker(manifest)
            Path(str(path)+".worker_finished").write_text("finished")
            return "123"
        with patch.object(controller,"ensure_catalogue",return_value=snapshot), \
             patch.object(main,"_load_catalogue",return_value=None), \
             patch.object(controller,"_download",side_effect=self.archive), \
             patch.object(main,"_submit",side_effect=submit) as submit_mock:
            result = controller._run_locked(args,main)
        self.assertEqual(result["status"],"complete")
        self.assertEqual(submit_mock.call_count,1)
        manifest = util.read_json(Path(result["jobs"][0]))
        self.assertEqual(set(manifest["results"]), {"UCS1","UCS99"})
        self.assertTrue(all(x["status"]=="complete" for x in manifest["results"].values()))

    def test_partial_resume_keeps_master_and_does_not_mask_twice(self):
        manifest = self.manifest()
        self.archive(manifest)
        actual_process = pipeline.process_sb
        attempts = []
        def process(*args, **kwargs):
            attempts.append(args[0]["source_name"])
            if len(attempts) == 2:
                return {"status":"partial"}
            return actual_process(*args, **kwargs)
        with patch.object(pipeline,"process_sb",side_effect=process):
            self.assertEqual(self.run_worker(manifest),config.PARTIAL_EXIT_CODE)
        manifest = util.read_json(Path(manifest["manifest_path"]))
        self.assertEqual(manifest["outcome"],"partial")
        self.assertEqual(self.run_worker(manifest),0)
        self.assertEqual(self.calls["dstools-create-model"],1)
        self.assertEqual(self.calls["dstools-insert-model"],2)

    def test_no_cleanup_unless_all_three_products_exist(self):
        manifest = self.manifest()
        self.archive(manifest)
        actual = self.fake_dstools
        def omit_ds(command,*args):
            if command[0] != "dstools-extract-ds":
                actual(command,*args)
        with patch.object(pipeline,"_container_command",side_effect=lambda *args:list(args)), \
             patch.object(pipeline,"_run_dstools",side_effect=omit_ds):
            worker.run_manifest(manifest)
        self.assertTrue((Path(manifest["shared_root"])/"master").is_dir())
        for source in self.sources:
            target = config.STAGING_ROOT/source["source_name"]/"LongObs"/self.obs["sb_name"]
            self.assertIsNotNone(pipeline._find_ms(target))
            self.assertFalse(worker.already_complete(source,self.obs))

    def test_state_failure_is_fatal_not_a_scientific_failure(self):
        manifest = self.manifest()
        self.archive(manifest)
        with patch.object(status,"record_step",side_effect=status.StateDatabaseError("unwritable")):
            with self.assertRaises(status.StateDatabaseError):
                self.run_worker(manifest)
        frozen = util.read_json(Path(manifest["manifest_path"]))
        self.assertEqual(frozen["fatal_state_error"],"unwritable")
        with self.assertRaises(status.StateDatabaseError):
            controller.wait_for_job(Path(manifest["manifest_path"]),1)

    def test_legacy_identity_checked_before_initialization(self):
        self.manifest()
        with status._connect() as db:
            db.execute("UPDATE sb_processing SET ms_name='different.ms.tar' WHERE source_name='UCS1'")
        with self.assertRaisesRegex(RuntimeError,"Changed filename"):
            controller.select_targets(self.group,False)

    def test_successful_private_retry_cleans_public_model(self):
        initial = self.manifest()
        self.archive(initial)
        self.fail_insert = {"UCS1"}
        self.run_worker(initial)
        retry = self.manifest([self.sources[0]],retry=True)
        controller.prepare(retry)
        self.fail_insert.clear()
        self.run_worker(retry)
        self.assertTrue(worker.already_complete(self.sources[0],self.obs))
        self.assertFalse((Path(initial["shared_root"])/"master").exists())

    def test_failed_queries_do_not_erase_successful_processing(self):
        manifest = self.manifest()
        self.archive(manifest)
        self.run_worker(manifest)
        catalogue = pd.DataFrame({"UCS_name":["UCS1"],"UCS":[1]})
        with patch.object(cross.casda_query,"query_source_table",side_effect=RuntimeError("timeout")):
            cross.ensure_catalogue(catalogue,lambda row:self.catalogue_source(1),update=True)
        self.assertTrue(worker.already_complete(self.sources[0],self.obs))

    def test_record_retry_only_selected_beam_and_source(self):
        self.manifest()
        for source in self.sources:
            worker._record_failure(source,self.obs,"insert_model","fixture")
        other = rows(beam=13)
        status.initialize_source_rows(self.sources[0],other)
        other_obs = cross.observation(other[0])
        worker._record_failure(self.sources[0],other_obs,"insert_model","fixture")
        status.prepare_observation_retry("UCS1",self.obs["sb_id"],"beam12")
        self.assertEqual(status.retry_stage("UCS1",self.obs["sb_id"],beam="beam12"),"processing_retry_ready")
        self.assertEqual(status.retry_stage("UCS1",self.obs["sb_id"],beam="beam13"),"processing_failed")
        self.assertEqual(status.retry_stage("UCS99",self.obs["sb_id"],beam="beam12"),"processing_failed")

    def test_two_batches_reuse_master_at_two_hour_cap(self):
        args = SimpleNamespace(begin=1,end=1,retry_existing=False,update_crosscatalogue=False,
            refresh_cache=False,crosscatalogue_only=False,submit=True,prepare_download=False,
            max_resubmits=1,controller_poll_interval=1)
        def submit(script):
            path = script.with_suffix(".json")
            manifest = util.read_json(path)
            self.assertEqual(manifest["job_walltime_hours"],2.0)
            self.run_worker(manifest)
            Path(str(path)+".worker_finished").write_text("finished")
            return "123"
        # Use the actual download function (including its shared-data reuse).
        class Client:
            def stage_data(inner,table):
                return ["https://example.invalid/"+self.obs["filename"]+suffix for suffix in ("", ".checksum")]
            def download_files(inner,urls,savedir):
                self.archive({"archive_path":str(Path(savedir)/self.obs["filename"])})
        with patch.object(config,"MAX_JOB_WALLTIME_HOURS",2.0), \
             patch.object(controller,"ensure_catalogue",return_value=dict(revision="test",sources=self.sources)), \
             patch.object(main,"_load_catalogue",return_value=None), \
             patch.object(controller.casda_query,"build_casda_client",return_value=Client()) as client, \
             patch.object(main,"_submit",side_effect=submit) as submission:
            plan = controller._run_locked(args,main)
        self.assertEqual(plan["status"],"complete")
        self.assertEqual(submission.call_count,2)
        self.assertEqual(client.call_count,1)
        self.assertEqual(self.calls["dstools-create-model"],1)
        jobs = [util.read_json(Path(path)) for path in plan["jobs"]]
        self.assertEqual([job["shared_model_built_this_job"] for job in jobs],[True,False])

    def test_submission_uncertain_never_submits_again(self):
        args = SimpleNamespace(begin=1,end=1,retry_existing=False,update_crosscatalogue=False,
            refresh_cache=False,crosscatalogue_only=False,submit=True,prepare_download=False,
            max_resubmits=1,controller_poll_interval=1)
        with patch.object(controller,"ensure_catalogue",return_value=dict(revision="test",sources=self.sources)), \
             patch.object(main,"_load_catalogue",return_value=None), \
             patch.object(controller,"_download",side_effect=self.archive), \
             patch.object(main,"_submit",side_effect=KeyboardInterrupt()) as submit:
            with self.assertRaises(KeyboardInterrupt):
                controller._run_locked(args,main)
            with self.assertRaisesRegex(RuntimeError,"Submission outcome is unknown"):
                controller._run_locked(args,main)
        self.assertEqual(submit.call_count,1)

    def test_cleanup_preserves_another_beams_checksum(self):
        target = config.STAGING_ROOT/"UCS1/LongObs/SB35429_beam12"
        target.mkdir(parents=True)
        mine = target.parent/self.obs["filename"]
        mine = Path(str(mine)+".checksum")
        other = target.parent/mine.name.replace("beam12","beam13")
        mine.write_text("mine")
        other.write_text("other")
        pipeline._discard_sb_for_redownload("UCS1",target,worker.product_dir(self.sources[0],self.obs),
                                           config.WSCLEAN_TEMP_ROOT/"UCS1_SB35429_beam12")
        self.assertFalse(mine.exists())
        self.assertTrue(other.exists())

    def multiple_observations(self, count):
        self.table = vstack([rows(sb=35000+i) for i in range(count)])
        observations = [cross.observation(row) for row in self.table]
        for source in self.sources:
            source["observations"] = copy.deepcopy(observations)
            self.table.write(source["query_cache"], format="ascii.ecsv", overwrite=True)
        return dict(revision="multi", sources=self.sources)

    def controller_args(self):
        return SimpleNamespace(begin=1,end=1,retry_existing=False,update_crosscatalogue=False,
            refresh_cache=False,crosscatalogue_only=False,submit=True,prepare_download=False,
            max_resubmits=2,controller_poll_interval=1)

    def fake_child(self, path, deadline, environment, log_path):
        log_path.write_text("child process fixture")
        self.assertEqual(environment["SLURM_CPUS_PER_TASK"], "8")
        self.assertEqual(environment["ASKAP_PARALLEL_SLOTS"], "1")
        return worker.run_manifest(util.read_json(path), deadline)

    def run_multi_controller(self, snapshot, submit):
        with patch.object(controller,"ensure_catalogue",return_value=snapshot), \
             patch.object(main,"_load_catalogue",return_value=None), \
             patch.object(controller,"_download",side_effect=self.archive), \
             patch.object(main,"_submit",side_effect=submit), \
             patch.object(worker,"_run_child",side_effect=self.fake_child), \
             patch.object(pipeline,"_container_command",side_effect=lambda *args:list(args)), \
             patch.object(pipeline,"_run_dstools",side_effect=self.fake_dstools):
            return controller._run_locked(self.controller_args(),main)

    def submit_batch(self, script):
        path = script.with_suffix(".json")
        job = util.read_json(path)
        worker.run_manifest(job)
        Path(str(path)+".worker_finished").write_text("finished")
        return "123"

    def test_four_observations_parallel_preprocessed_copies_and_resource_shares(self):
        snapshot = self.multiple_observations(4)
        barrier = threading.Barrier(4)
        original_child = self.fake_child
        def simultaneous(*args):
            self.assertEqual(args[2]["SLURM_MEM_PER_NODE"], "20480")
            barrier.wait(timeout=5)
            return original_child(*args)
        self.fake_child = simultaneous
        with patch.object(config,"PARALLEL_SLOTS",4), \
             patch.dict(worker.os.environ, {"SLURM_MEM_PER_NODE":"81920", "SLURM_CPUS_PER_TASK":"32"}):
            result = self.run_multi_controller(snapshot,self.submit_batch)
        self.assertEqual(len(result["jobs"]),1)
        job = util.read_json(Path(result["jobs"][0]))
        self.assertEqual(job["parallel_slots"],4)
        self.assertEqual(job["slot_load_hours"],[3.0]*4)
        self.assertEqual(job["job_walltime_hours"],5.0)
        self.assertEqual(self.calls["dstools-create-model"],4)
        self.assertEqual(self.calls["dstools-insert-model"],8)
        script = Path(job["script_path"]).read_text()
        self.assertIn("#SBATCH --cpus-per-task=32",script)
        self.assertIn("export ASKAP_PARALLEL_SLOTS=4",script)
        self.assertNotIn("heartbeat",script)
        for task in controller.job_tasks(job):
            self.assertTrue(all(r["status"]=="complete" for r in task["results"].values()))

    def test_wsclean_slot_wrapper_fills_only_missing_options(self):
        wrapper = config.PROGRAM_DIR/"wsclean_slot/wsclean"
        self.assertTrue(wrapper.stat().st_mode & 0o111)
        subprocess.run(["bash","-n",str(wrapper)],check=True)
        # Execute the actual wrapper logic against an argument-printing stub.
        stub = self.root/"wsclean-stub"
        stub.write_text('#!/bin/bash\nprintf "%s\\n" "$@"\n')
        stub.chmod(0o755)
        test_wrapper = self.root/"wrapper"
        test_wrapper.write_text(wrapper.read_text().replace("/opt/wsclean/bin/wsclean",str(stub)))
        environment = dict(worker.os.environ, ASKAP_WSCLEAN_SLOT_THREADS="8", ASKAP_WSCLEAN_ABS_MEM="15")
        result = subprocess.run(["bash",str(test_wrapper),"-predict","test.ms"],
            env=environment,capture_output=True,text=True,check=True)
        self.assertEqual(result.stdout.splitlines(),["-j","8","-abs-mem","15","-predict","test.ms"])
        result = subprocess.run(["bash",str(test_wrapper),"-j","8","-abs-mem","15","test.ms"],
            env=environment,capture_output=True,text=True,check=True)
        self.assertEqual(result.stdout.splitlines(),["-j","8","-abs-mem","15","test.ms"])

    def test_container_uses_slot_wrapper_without_host_path_leak(self):
        with patch.dict(worker.os.environ,{"SLURM_MEM_PER_NODE":"163840"}):
            environment = worker.slot_environment(4)
        self.assertEqual(environment["SLURM_MEM_PER_NODE"],"40960")
        self.assertEqual(environment["ASKAP_WSCLEAN_ABS_MEM"],"30")
        with patch.dict(worker.os.environ,environment):
            command = pipeline._container_command("dstools-insert-model","test.ms")
            self.assertIn("/bin/bash",command)
            self.assertIn('exec "$@"',command[-4])
            self.assertTrue(any("ASKAP_WSCLEAN_SLOT_THREADS=8" == arg for arg in command))
            create = pipeline.create_model_command(Path("test.ms"),Path("temp"))
            self.assertIn("--abs-mem",create)
            self.assertEqual(create[create.index("--abs-mem")+1],"30")

    def test_parallel_work_packed_into_bounded_waves_and_continues_other_groups(self):
        snapshot = self.multiple_observations(12)
        # Four lanes each fit two groups (2h build + 2*0.5h targets), plus 2h overhead.
        self.fail_insert = {"UCS99"}
        with patch.object(config,"PARALLEL_SLOTS",4), patch.object(config,"MAX_JOB_WALLTIME_HOURS",8):
            result = self.run_multi_controller(snapshot,self.submit_batch)
        jobs = [util.read_json(Path(path)) for path in result["jobs"]]
        self.assertEqual([len(job["tasks"]) for job in jobs],[8,4])
        self.assertEqual([job["job_walltime_hours"] for job in jobs],[8.0,5.0])
        self.assertEqual(self.calls["dstools-create-model"],12)
        for job in jobs:
            for task in controller.job_tasks(job):
                self.assertEqual(task["results"]["UCS1"]["status"],"complete")
                self.assertEqual(task["results"]["UCS99"]["status"],"failed")
                self.assertTrue((Path(task["shared_root"])/"master").is_dir())

    def test_batch_partial_continuation_skips_finished_observations(self):
        snapshot = self.multiple_observations(4)
        first = []
        child = self.fake_child
        def partial_once(path, deadline, environment, log_path):
            if not first:
                first.append(str(path))
                task = util.read_json(path)
                task["outcome"] = "partial"
                util.atomic_write_json(path,task)
                log_path.write_text("partial fixture")
                return config.PARTIAL_EXIT_CODE
            return child(path,deadline,environment,log_path)
        self.fake_child = partial_once
        with patch.object(config,"PARALLEL_SLOTS",4):
            result = self.run_multi_controller(snapshot,self.submit_batch)
        job = util.read_json(Path(result["jobs"][0]))
        self.assertEqual(job["continuation_count"],1)
        self.assertEqual(self.calls["dstools-create-model"],4)
        self.assertEqual(len(result["jobs"]),1)
        self.assertEqual(job["job_walltime_hours"],5.0)
        self.assertEqual(job["parallel_slots"],1)

    def test_retained_preprocessed_model_does_not_request_imaging_time_again(self):
        manifest = self.manifest()
        self.archive(manifest)
        self.fail_insert = {"UCS1"}
        self.run_worker(manifest)
        next_manifest = self.manifest([self.sources[0]])
        self.assertEqual(controller.task_hours(next_manifest),0.5)
        controller.schedule_job(next_manifest)
        self.assertEqual(next_manifest["job_walltime_hours"],2.5)

    def test_default_four_slots_48_hour_cap_and_cli_validation(self):
        environment = {k:v for k,v in worker.os.environ.items() if not k.startswith("ASKAP_")}
        result = subprocess.run([sys.executable,"-c",
            "import config; print(config.PARALLEL_SLOTS, config.MAX_JOB_WALLTIME_HOURS, config.SBATCH_MEM, config.CASDA_RADIUS_FREQUENCY_MODE)"],
            cwd=str(config.PROGRAM_DIR),env=environment,capture_output=True,text=True,check=True)
        self.assertEqual(result.stdout.strip(),"4 48.0 160G central")
        for slots in ("0","5"):
            result = subprocess.run([sys.executable,str(config.PROGRAM_DIR/"ozstar_main.py"),
                "--parallel-slots",slots,"--crosscatalogue-only"],
                env=environment,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertIn("slots",result.stderr)

    def test_central_default_and_frequency_cli_environment_precedence(self):
        # Fresh interpreters verify the actual deployed default, independent
        # of the explicit top fixtures used for old-cache regression tests.
        environment = {k:v for k,v in worker.os.environ.items() if not k.startswith("ASKAP_")}
        script = (
            "import config, casda_query, ozstar_main, shared_controller; "
            "shared_controller.run = lambda args: print("
            "config.CASDA_RADIUS_FREQUENCY_MODE, "
            "casda_query.frequency_selection(0.21, 0.3)['radius_frequency_mode']); "
            "ozstar_main.main()"
        )
        for env_mode, cli_mode, expected in (
                (None, None, "central"), ("top", None, "top"),
                ("top", "central", "central"), ("central", "top", "top")):
            with self.subTest(environment=env_mode, cli=cli_mode):
                child_env = dict(environment)
                if env_mode is not None:
                    child_env["ASKAP_CASDA_RADIUS_FREQUENCY_MODE"] = env_mode
                arguments = ["--crosscatalogue-only"]
                if cli_mode is not None:
                    arguments += ["--radius-frequency-mode", cli_mode]
                result = subprocess.run([sys.executable, "-c", script, *arguments],
                    cwd=str(config.PROGRAM_DIR), env=child_env,
                    capture_output=True, text=True, check=True)
                self.assertEqual(result.stdout.strip(), expected + " " + expected)

    def test_standalone_downloader_modes_match_pipeline_without_network(self):
        # Execute only the numeric selection section, never login/query/download.
        path = config.PROGRAM_DIR.parent / "Download_ASKAP_Visibility_Remote.py"
        tree = ast.parse(path.read_text())
        assignments = {
            node.targets[0].id: node for node in tree.body
            if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
        }
        self.assertEqual(ast.literal_eval(assignments["RADIUS_FREQUENCY_MODE"].value), "central")
        start = tree.body.index(assignments["em_min_metres"])
        end = tree.body.index(assignments["inside_frequency_dependent_radius"])
        numeric_code = compile(ast.Module(body=tree.body[start:end + 1], type_ignores=[]), str(path), "exec")
        table = vstack([rows(), rows()])
        table["em_min"] = [299792458.0/1.6e9, 299792458.0/1.6e9]
        table["em_max"] = [299792458.0/1.0e9, 299792458.0/1.0e9]
        separation = np.array([0.5, 0.58])
        for mode in ("central", "top"):
            namespace = dict(np=np, data=table, separation_deg=separation,
                RADIUS_FREQUENCY_MODE=mode,
                wavelength_column_in_metres=lambda data, column: np.asarray(data[column], dtype=float))
            for name in ("REFERENCE_FWHM_DEG", "REFERENCE_FREQUENCY_GHZ", "SPEED_OF_LIGHT_METRES_PER_SECOND"):
                namespace[name] = ast.literal_eval(assignments[name].value)
            exec(numeric_code, namespace)
            expected = cross.casda_query.frequency_selection(
                np.asarray(table["em_min"]), np.asarray(table["em_max"]), mode)
            np.testing.assert_allclose(namespace["half_power_radius_deg"], expected["half_power_radius_deg"])
            self.assertEqual(sum(namespace["inside_frequency_dependent_radius"]), 2 if mode == "central" else 1)

    def gate9_phases(self, label):
        """Use the real shared controller/worker/retry state machine, fake science."""
        root = self.root / "gate9-validation"
        work = root / label
        names = (1, 99) if label == "shared-recovery" else (1,)
        with ExitStack() as stack:
            values = dict(WORK=work, STATE_ROOT=work/"state", STAGING_ROOT=work/"staging",
                PRODUCT_ROOT=work/"products", LOG_ROOT=work/"logs", WSCLEAN_TEMP_ROOT=work/"staging/_wsclean_tmp",
                SBATCH_MEM="40G", SBATCH_TMP="10G", MAX_MS_PER_JOB=1)
            for key, value in values.items():
                stack.enter_context(patch.object(config, key, value))
            stack.enter_context(patch.object(util, "PRODUCT_ROOT", config.PRODUCT_ROOT))
            stack.enter_context(patch.object(util, "STATE_ROOT", config.STATE_ROOT))
            stack.enter_context(patch.object(status, "DATABASE_PATH", config.STATE_ROOT/"sb_processing.sqlite"))
            config.ensure_directories()
            self.sources = [self.source(n) for n in names]
            self.group = dict(observation=self.obs, sources=self.sources,
                crosscatalogue_revision="fixture", seed_source="UCS1")
            case = dict(gate9.CASES[label], seed=1, target="UCS1", sources=[s["source_name"] for s in self.sources],
                sb_id=self.obs["sb_id"], beam=self.obs["beam"], key=self.obs["key"])
            stack.enter_context(patch.dict(gate9.CASES, {label: case}))
            fault = work/"state/gate9-fault-config.json"
            util.atomic_write_json(fault, {"gate":"gate9", "work":str(work), "rules":[
                dict(source_name="UCS1", sb_id=self.obs["sb_id"], beam=self.obs["beam"],
                     retry_stage=stage, step=step) for stage,step in case["faults"]]})
            stack.enter_context(patch.dict(worker.os.environ, {"ASKAP_GATE9_FAULT_CONFIG":str(fault),
                                                              "ASKAP_GATE6_FAULT_CONFIG":""}))
            snapshot = dict(revision="fixture", sources=self.sources)
            stack.enter_context(patch.object(controller,"ensure_catalogue",return_value=snapshot))
            stack.enter_context(patch.object(main,"_load_catalogue",return_value=None))
            stack.enter_context(patch.object(pipeline,"_container_command",side_effect=lambda *args:list(args)))
            stack.enter_context(patch.object(pipeline,"_run_dstools",side_effect=self.fake_dstools))
            def download(manifest):
                self.archive(manifest)
                util.atomic_write_json(Path(manifest["shared_root"])/"download-ready.json", {"fixture":True})
            stack.enter_context(patch.object(controller,"_download",side_effect=download))
            def submit(script):
                path = script.with_suffix(".json")
                self.assertEqual(worker.run_manifest(util.read_json(path)),0)
                Path(str(path)+".worker_finished").write_text("finished")
                return str(len(gate9.submissions(work))+1)
            stack.enter_context(patch.object(main,"_submit",side_effect=submit))
            runner = gate9.Runner(root, {}, {"completed_phases":[]}, 1, 60)
            def run(case_name, retry=False, interrupt=False):
                args = self.controller_args()
                args.shared_observation_key = self.obs["key"]
                args.retry_existing = retry
                controller._run_locked(args, main)
                if interrupt:
                    runner.state["interruption"] = {"submission":gate9.submissions(work)}
            stack.enter_context(patch.object(runner,"run",side_effect=run))
            phases = gate9.PHASES[:4] if label == "shared-recovery" else gate9.PHASES[4:]
            for phase in phases:
                runner.phase(phase)
            self.assertEqual(len(gate9.submissions(work)),3)
            self.assertEqual(gate9.download_count(work),2)

    def test_gate9_shared_recovery_and_completed_idempotence(self):
        self.gate9_phases("shared-recovery")
        self.assertEqual(self.calls["dstools-create-model"],2)

    def test_gate9_exhaustion_and_no_further_submissions(self):
        self.gate9_phases("exhaustion")
        self.assertEqual(self.calls["dstools-create-model"],0)

    def test_gate9_hook_is_opt_in_scoped_and_forwarded(self):
        with patch.dict(worker.os.environ, {"ASKAP_GATE9_FAULT_CONFIG":""}):
            self.assertNotIn("ASKAP_GATE9_FAULT_CONFIG", main._worker_runtime_environment())
            pipeline._inject_gate9_failure("UCS1","SB1","beam01","none","insert_model",[],self.root/"unused.log")
            self.assertFalse((self.root/"unused.log").exists())
        fault = config.STATE_ROOT/"gate9-fault-config.json"
        with patch.dict(worker.os.environ, {"ASKAP_GATE9_FAULT_CONFIG":str(fault)}):
            self.assertEqual(main._worker_runtime_environment()["ASKAP_GATE9_FAULT_CONFIG"],str(fault))
            with self.assertRaisesRegex(pipeline.ProcessingError,"isolated"):
                pipeline._inject_gate9_failure("UCS1","SB1","beam01","none","insert_model",[],self.root/"unused.log")

    def test_gate9_driver_controller_sigint_does_not_delete_submission(self):
        root = self.root/"gate9-validation-driver"
        work = root/"shared-recovery"
        (work/"logs").mkdir(parents=True)
        program = root/"program"
        program.mkdir()
        (program/"ozstar_main.py").write_text(
            "import os,json,time\nfrom pathlib import Path\n"
            "p=Path(os.environ['ASKAP_WORK'])/'state/jobs/fixture.json.submission.json'\n"
            "p.parent.mkdir(parents=True)\nt=p.with_suffix('.tmp')\n"
            "t.write_text(json.dumps({'job_id':'999'}))\nt.replace(p)\n"
            "while True: time.sleep(1)\n")
        settings = dict(catalogue="fixture",time_csv="fixture",container="fixture")
        runner = gate9.Runner(root,settings,{},0.02,5)
        with patch.object(gate9,"PROGRAM",program):
            runner.run("shared-recovery",interrupt=True)
        self.assertEqual(list(gate9.submissions(work).values()),["999"])
        self.assertEqual(runner.state["interruption"]["signal"],"SIGINT (controller only)")
        self.assertTrue((root/"state/gate9-checkpoint.json").is_file())

    def test_gate9_paths_and_case_environment_do_not_leak(self):
        with self.assertRaises(ValueError):
            gate9.isolated_work(config.PROGRAM_DIR/"gate9-validation")
        with self.assertRaises(ValueError):
            gate9.isolated_work(self.root/"ordinary")
        settings = dict(catalogue="c",time_csv="t",container="s")
        with patch.dict(worker.os.environ,{"ASKAP_GATE6_FAULT_CONFIG":"old"}):
            env = gate9.environment(self.root/"gate9-validation",settings)
        self.assertNotIn("ASKAP_GATE6_FAULT_CONFIG",env)
        self.assertEqual(env["ASKAP_SBATCH_MEM"],"40G")
        self.assertEqual(env["ASKAP_SBATCH_TMP"],"10G")
        self.assertEqual(env["ASKAP_CASDA_RADIUS_FREQUENCY_MODE"],"central")
        self.assertNotEqual(worker.os.environ.get("ASKAP_WORK"),env["ASKAP_WORK"])

    def test_deployment_includes_standalone_without_network(self):
        # Stub the transport, not the source-file check: verify the real package.
        bin_dir = self.root / "deploy-bin"
        bin_dir.mkdir()
        stub = bin_dir / "sshpass"
        stub.write_text('#!/bin/bash\nprintf \'%s\\n\' "$*" >> "$DEPLOY_TEST_LOG"\n')
        stub.chmod(0o700)
        record = self.root / "deploy-commands.log"
        environment = dict(worker.os.environ, PATH=str(bin_dir) + ":" + worker.os.environ["PATH"],
            TRANSFER_PASSWORD="test-only-not-a-real-password", DEPLOY_TEST_LOG=str(record),
            OZSTAR_WORK="/fred/test/ASKAP-UCDs-shared", OZSTAR_SSH_USER="test-user",
            OZSTAR_SSH_HOST="example.invalid")
        script = config.PROGRAM_DIR / "deploy_to_ozstar.sh"
        subprocess.run(["bash", str(script)], env=environment, check=True,
            capture_output=True, text=True)
        commands = record.read_text().splitlines()
        self.assertEqual(len(commands), 3, commands)
        self.assertIn(" ssh ", commands[0])
        self.assertIn(" scp -r -p ", commands[1])
        self.assertIn(str(config.PROGRAM_DIR), commands[1])
        self.assertIn(" scp -p ", commands[2])
        self.assertIn("Download_ASKAP_Visibility_Remote.py", commands[2])
        self.assertTrue(all("test-user@example.invalid" in c for c in commands))
        self.assertTrue(all(environment["TRANSFER_PASSWORD"] not in c for c in commands))
        # An incomplete checkout must fail before even the mocked SSH executes.
        incomplete = self.root / "incomplete" / "ozstar_askap"
        incomplete.mkdir(parents=True)
        copied_script = incomplete / script.name
        copied_script.write_text(script.read_text())
        result = subprocess.run(["bash", str(copied_script)], env=environment,
            capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Standalone downloader not found", result.stderr)
        self.assertEqual(record.read_text().splitlines(), commands)

    def test_batch_fatal_state_error_stops_controller(self):
        snapshot = self.multiple_observations(2)
        def fatal_child(path,deadline,environment,log_path):
            task = util.read_json(path)
            task["fatal_state_error"] = "database unavailable"
            util.atomic_write_json(path,task)
            log_path.write_text("fatal fixture")
            return 1
        self.fake_child = fatal_child
        with patch.object(config,"PARALLEL_SLOTS",2):
            with self.assertRaises(status.StateDatabaseError):
                self.run_multi_controller(snapshot,self.submit_batch)
        plan = util.read_json(config.STATE_ROOT/"shared-controller.json")
        job = util.read_json(Path(plan["active_manifest"]))
        self.assertEqual(job["fatal_state_error"],"database unavailable")

    def test_two_hour_test_cap_still_uses_four_independent_slots(self):
        snapshot = self.multiple_observations(4)
        with patch.object(config,"PARALLEL_SLOTS",4), patch.object(config,"MAX_JOB_WALLTIME_HOURS",2):
            result = self.run_multi_controller(snapshot,self.submit_batch)
        jobs = [util.read_json(Path(path)) for path in result["jobs"]]
        self.assertEqual(len(jobs),2)  # One companion per observation, then reuse for the other.
        self.assertTrue(all(job["parallel_slots"]==4 and job["job_walltime_hours"]==2 for job in jobs))
        self.assertEqual(self.calls["dstools-create-model"],4)

    def test_interrupted_batch_planning_reuses_reserved_manifests(self):
        snapshot = self.multiple_observations(4)
        make = controller.make_manifest
        made = []
        def interrupt(group,*args):
            if len(made)==1:
                raise KeyboardInterrupt()
            task = make(group,*args)
            made.append(task["manifest_path"])
            return task
        with patch.object(config,"PARALLEL_SLOTS",4), \
             patch.object(controller,"make_manifest",side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.run_multi_controller(snapshot,self.submit_batch)
        plan = util.read_json(config.STATE_ROOT/"shared-controller.json")
        self.assertEqual(plan["planning_tasks"],made)
        with patch.object(config,"PARALLEL_SLOTS",4):
            result = self.run_multi_controller(snapshot,self.submit_batch)
        job = util.read_json(Path(result["jobs"][0]))
        self.assertEqual(job["tasks"][0]["manifest_path"],made[0])
        self.assertEqual(self.calls["dstools-create-model"],4)

    def test_abnormal_observation_process_does_not_stop_other_lanes(self):
        snapshot = self.multiple_observations(4)
        child = self.fake_child
        def broken(path,deadline,environment,log_path):
            if "SB35000_" in path.name:
                log_path.write_text("unexpected subprocess crash")
                return 1
            return child(path,deadline,environment,log_path)
        self.fake_child = broken
        with patch.object(config,"PARALLEL_SLOTS",4):
            result = self.run_multi_controller(snapshot,self.submit_batch)
        job = util.read_json(Path(result["jobs"][0]))
        tasks = controller.job_tasks(job)
        self.assertTrue(all(x["status"]=="failed" for x in tasks[0]["results"].values()))
        self.assertTrue(all(x["status"]=="complete" for task in tasks[1:] for x in task["results"].values()))

    def test_real_child_process_entrypoint_and_frozen_paths(self):
        task = self.manifest()
        self.archive(task)
        self.run_worker(task)
        path = Path(task["manifest_path"])
        environment = worker.slot_environment(1)
        environment.update(ASKAP_WORK=str(self.root), DSTOOLS_CONTAINER=str(config.CONTAINER),
            ASKAP_TIME_CSV=str(config.TIME_CSV_PATH), ASKAP_CREATE_MODEL_THREADS="8", ASKAP_CPU_PER_SLOT="8")
        rc = worker._run_child(path,time.monotonic()+60,environment,self.root/"actual-child.log")
        self.assertEqual(rc,0,(self.root/"actual-child.log").read_text())
        self.assertEqual(util.read_json(path)["outcome"],"finished")

    def test_48_hour_capacity_splits_targets_and_rejects_larger_cap(self):
        self.sources = [self.source(number) for number in range(1,101)]
        snapshot = dict(revision="many-targets", sources=self.sources)
        args = self.controller_args()
        args.submit = False
        with patch.object(config,"MAX_JOB_WALLTIME_HOURS",48), \
             patch.object(controller,"ensure_catalogue",return_value=snapshot), \
             patch.object(main,"_load_catalogue",return_value=None):
            plan = controller._run_locked(args,main)
        task = util.read_json(Path(plan["active_manifest"]))
        self.assertEqual(task["job_walltime_hours"],48)
        self.assertEqual(len(task["sources"]),88)
        self.assertEqual(len(task["all_sources"]),100)
        for cap in (49,96,float("nan")):
            with patch.object(config,"MAX_JOB_WALLTIME_HOURS",cap):
                with self.assertRaisesRegex(ValueError,"<= 48"):
                    controller.run(args)


if __name__ == "__main__":
    unittest.main()
