"""Unit tests for metrics calculation functions."""
import pytest
import sys
from pathlib import Path

# Add tests directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from metrics.metrics_calculator import (
    calculate_completeness,
    calculate_accuracy,
    calculate_f1_score,
    calculate_schema_compliance_rate,
    calculate_repair_rate,
    calculate_latency,
    calculate_ocr_fallback_rate,
    calculate_geocoding_cache_hit_rate,
    calculate_llm_token_usage,
    calculate_all_metrics
)


@pytest.mark.unit
class TestMetricsCalculator:
    """Test cases for metrics calculation functions.

    Tests completeness, accuracy, F1-score, schema compliance, repair rates,
    latency, OCR fallback, cache hit rates, and token usage calculations.
    """
    
    def test_calculate_completeness(self):
        """Test completeness (recall) calculation."""
        golden = {
            "demographic": {"name": "John", "age_years": 25},
            "spatial": {"last_seen_city": "Richmond"}
        }
        generated = {
            "demographic": {"name": "John", "age_years": 25}
        }
        
        completeness = calculate_completeness(generated, golden)
        print(f"\n[PASS] Completeness (Recall): {completeness:.4f}")
        # Should be less than 1.0 since spatial field is missing
        assert 0.0 <= completeness <= 1.0
        assert completeness < 1.0
    
    def test_calculate_accuracy(self):
        """Test accuracy (precision) calculation."""
        golden = {
            "demographic": {"name": "John", "age_years": 25}
        }
        generated = {
            "demographic": {"name": "John", "age_years": 25, "extra_field": "value"}
        }
        
        accuracy = calculate_accuracy(generated, golden)
        print(f"\n[PASS] Accuracy (Precision): {accuracy:.4f}")
        # Should be high since matching fields are correct
        assert 0.0 <= accuracy <= 1.0
    
    def test_calculate_f1_score(self):
        """Test F1-score calculation."""
        precision = 0.8
        recall = 0.9
        f1 = calculate_f1_score(precision, recall)
        
        print(f"\n[PASS] F1-Score: {f1:.4f} (Precision: {precision:.2f}, Recall: {recall:.2f})")
        assert 0.0 <= f1 <= 1.0
        # F1 should be between precision and recall
        assert min(precision, recall) <= f1 <= max(precision, recall)
    
    def test_calculate_f1_score_zero(self):
        """Test F1-score with zero precision and recall."""
        f1 = calculate_f1_score(0.0, 0.0)
        assert f1 == 0.0
    
    def test_calculate_schema_compliance_rate(self):
        """Test schema compliance rate calculation."""
        records = [{}, {}, {}]
        validation_results = [[], ["error"], []]
        
        compliance = calculate_schema_compliance_rate(records, validation_results)
        print(f"\n[PASS] Schema Compliance Rate: {compliance:.4f} (2/3 records passed)")
        # 2 out of 3 records passed validation
        assert compliance == pytest.approx(2.0 / 3.0)
    
    def test_calculate_repair_rate(self):
        """Test repair rate calculation."""
        repair_counts = [0, 1, 0, 2, 0]
        
        repair_rate = calculate_repair_rate(repair_counts)
        print(f"\n[PASS] Repair Rate: {repair_rate:.4f} (2/5 records required repair)")
        # 2 out of 5 records required repair
        assert repair_rate == pytest.approx(2.0 / 5.0)
    
    def test_calculate_latency(self):
        """Test latency calculation."""
        timings = [1.0, 2.0, 3.0, 4.0, 5.0]
        
        latency = calculate_latency(timings)
        print(f"\n[PASS] Latency - Mean: {latency['mean']:.2f}s, Median: {latency['median']:.2f}s, "
              f"Min: {latency['min']:.2f}s, Max: {latency['max']:.2f}s")
        assert latency["mean"] == 3.0
        assert latency["median"] == 3.0
        assert latency["min"] == 1.0
        assert latency["max"] == 5.0
    
    def test_calculate_ocr_fallback_rate(self):
        """Test OCR fallback rate calculation."""
        records = [
            {"provenance": {"extraction_method": "pdfminer"}},
            {"provenance": {"extraction_method": "ocr"}},
            {"provenance": {"extraction_method": "pypdf2"}},
            {"_fulltext": "OCR extracted text"}
        ]
        
        ocr_rate = calculate_ocr_fallback_rate(records)
        print(f"\n[PASS] OCR Fallback Rate: {ocr_rate:.4f} (2/4 records used OCR)")
        # 2 out of 4 records used OCR
        assert ocr_rate == pytest.approx(2.0 / 4.0)
    
    def test_calculate_geocoding_cache_hit_rate(self):
        """Test geocoding cache hit rate calculation."""
        cache_stats = {"hits": 8, "misses": 2}
        
        hit_rate = calculate_geocoding_cache_hit_rate(cache_stats)
        print(f"\n[PASS] Geocoding Cache Hit Rate: {hit_rate:.4f} (8/10 requests were cache hits)")
        # 8 out of 10 requests were cache hits
        assert hit_rate == pytest.approx(0.8)
    
    def test_calculate_llm_token_usage(self):
        """Test LLM token usage calculation."""
        records = [
            {"audit": {"tokens": {"input": 100, "output": 50, "total": 150}}},
            {"audit": {"tokens": {"input": 200, "output": 100, "total": 300}}}
        ]
        
        token_usage = calculate_llm_token_usage(records)
        print(f"\n[PASS] LLM Token Usage - Input: {token_usage['input_tokens']:.0f}, "
              f"Output: {token_usage['output_tokens']:.0f}, Total: {token_usage['total_tokens']:.0f}")
        assert token_usage["input_tokens"] == 150.0
        assert token_usage["output_tokens"] == 75.0
        assert token_usage["total_tokens"] == 225.0
    
    def test_calculate_all_metrics(self):
        """Test calculation of all metrics."""
        generated_records = [
            {"demographic": {"name": "John", "age_years": 25}},
            {"demographic": {"name": "Jane", "age_years": 30}}
        ]
        golden_records = [
            {"demographic": {"name": "John", "age_years": 25, "gender": "male"}},
            {"demographic": {"name": "Jane", "age_years": 30, "gender": "female"}}
        ]
        validation_results = [[], []]
        repair_counts = [0, 1]
        timings = [1.0, 2.0]
        cache_stats = {"hits": 5, "misses": 5}
        
        metrics = calculate_all_metrics(
            generated_records,
            golden_records,
            validation_results,
            repair_counts,
            timings,
            cache_stats
        )
        
        print("\n" + "="*60)
        print("ALL METRICS SUMMARY")
        print("="*60)
        for key, value in sorted(metrics.items()):
            if isinstance(value, float):
                print(f"  {key:30s}: {value:.4f}")
            else:
                print(f"  {key:30s}: {value}")
        print("="*60)
        
        assert "completeness" in metrics
        assert "accuracy" in metrics
        assert "f1_score" in metrics
        assert "schema_compliance_rate" in metrics
        assert "repair_rate" in metrics
        assert "latency_mean" in metrics
        assert "geocoding_cache_hit_rate" in metrics

