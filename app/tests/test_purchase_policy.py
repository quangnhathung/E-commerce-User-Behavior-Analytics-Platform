"""Precision operating points and frozen archive regression without Spark."""
import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

from helpers import APP
from ecommerce_prediction.contracts import THRESHOLDS, confusion
from ecommerce_purchase_policy.contracts import TARGETS, meets, select, validate_grid


def grid_for(values):
    p = sum(label for score, label in values)
    n = len(values) - p
    result = []
    for threshold in THRESHOLDS:
        tp = sum(score >= threshold and label == 1 for score, label in values)
        fp = sum(score >= threshold and label == 0 for score, label in values)
        result.append({"threshold": threshold, **confusion(tp, fp, p-tp, n-fp)})
    return result


class PolicyContractTests(unittest.TestCase):
    def test_selects_highest_recall_feasible_point_and_higher_threshold_tie(self):
        grid = grid_for([(0.96, 1)]*8 + [(.94, 1)]*2 + [(.96, 0)]*2 + [(.94, 0)]*5 + [(.1, 0)]*104)
        selected = select(grid)
        self.assertEqual(.96, selected['threshold'])
        self.assertEqual(.8, selected['validation']['precision'])
        self.assertEqual(.8, selected['validation']['recall'])
        self.assertEqual({'minimum_precision':.7,'minimum_recall':.1}, TARGETS)

    def test_recall_tie_prefers_precision(self):
        grid=grid_for([(.9,1)]*3+[(.85,0)]+[(.1,0)]*3)
        self.assertEqual(.9, select(grid)['threshold'])
        self.assertEqual(1, select(grid)['validation']['recall'])

    def test_infeasible_grid_does_not_publish_a_fake_threshold(self):
        value=select(grid_for([(.5,1)]+[(.5,0)]*10))
        self.assertIsNone(value['threshold'])
        self.assertIsNone(value['validation'])
        self.assertFalse(value['validation_targets_met'])

    def test_zero_predictions_is_not_a_precision_success(self):
        self.assertFalse(meets(confusion(0,0,1,100)))
        self.assertTrue(meets(confusion(7,3,63,100)))
        self.assertFalse(meets(confusion(7,3,64,100)))

    def test_missing_grid_nonfinite_or_inconsistent_confusion_is_rejected(self):
        grid=grid_for([(.9,1),(.1,0)])
        bad=copy.deepcopy(grid);bad[90]['precision']=float('nan')
        inconsistent=copy.deepcopy(grid);inconsistent[50]['tn']+=1
        for value in (grid[:-1],bad,inconsistent,grid_for([(.9,1)])):
            with self.assertRaises(ValueError):validate_grid(value)

    def test_nonmonotone_threshold_counts_are_rejected(self):
        grid=grid_for([(.9,1),(.1,0)])
        grid[99]={"threshold":.99,**confusion(1,0,0,1)}
        with self.assertRaisesRegex(ValueError,'monotone'):validate_grid(grid)

    def test_existing_validation_reference_selects_096_without_using_test(self):
        # Counts at the three reported operating points; lower thresholds are infeasible.
        grid=[]
        for threshold in THRESHOLDS:
            tp,fp=(142,189) if threshold<=.93 else (136,165) if threshold<=.94 else (116,78) if threshold<=.95 else (90,23) if threshold<=.96 else (0,0)
            grid.append({'threshold':threshold,**confusion(tp,fp,729-tp,2559426-fp)})
        winner=select(grid)
        self.assertEqual(.96,winner['threshold'])
        self.assertAlmostEqual(90/113,winner['validation']['precision'])
        self.assertAlmostEqual(90/729,winner['validation']['recall'])


class PolicyPackagingTests(unittest.TestCase):
    def test_all_four_original_profiles_frozen_and_new_profile_reproducible(self):
        spec=importlib.util.spec_from_file_location('policy_packager',APP/'package_code.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        expected={'ingest':'ae42a5981bc20e1295df1dabbfab7e553af0f133a39c97fb52fa795ceafcb554',
            'streaming':'009c7d7d1d411e3f1810096f9c2954235ee6b92c7a3789514fe026b9af3516ac',
            'ml':'b3991c1379b84c0a440ce9ad023c411221e357dca6750cbd86bf917c6af831a9',
            'prediction':'a8b1243293bd1ed091ccb1cbcf1673ca8437d4ef48f0e50b1749d72ec3c7e4e0'}
        with tempfile.TemporaryDirectory() as directory:
            for profile,checksum in expected.items():
                self.assertEqual(checksum,module.package_code(Path(directory)/(profile+'.zip'),profile=profile)['sha256'])
            first=module.package_code(Path(directory)/'policy.zip',profile='purchase-policy')
            self.assertEqual(first['sha256'],module.package_code(Path(directory)/'second.zip',profile='purchase-policy')['sha256'])
            with zipfile.ZipFile(first['archive']) as archive:
                self.assertIn('ecommerce_purchase_policy/workflow.py',archive.namelist())
                self.assertFalse(any(name.startswith('ecommerce_streaming/') for name in archive.namelist()))
                for name in archive.namelist():compile(archive.read(name),name,'exec')
