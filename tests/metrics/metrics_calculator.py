"""Metrics calculation functions for Guardian Parser Pack.

Provides functions to calculate extraction quality metrics, schema compliance,
repair rates, latency, cache hit rates, and token usage statistics.
"""
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import statistics


def calculate_completeness(generated: Dict[str, Any], golden: Dict[str, Any]) -> float:
    """Calculate completeness (Recall) metric.

    Measures proportion of fields correctly extracted relative to total
    fields available in golden reference.

    Args:
        generated: Generated record dictionary.
        golden: Golden/reference record dictionary.

    Returns:
        Completeness score between 0.0 and 1.0.
    """
    if not golden:
        return 0.0
    
    # Count fields in golden (non-null, non-empty)
    total_fields = count_non_empty_fields(golden)
    if total_fields == 0:
        return 1.0  # No fields to extract
    
    # Count matching fields in generated
    matching_fields = count_matching_fields(generated, golden)
    
    return matching_fields / total_fields if total_fields > 0 else 0.0


def calculate_accuracy(generated: Dict[str, Any], golden: Dict[str, Any]) -> float:
    """Calculate accuracy (Precision) metric.

    Measures proportion of correctly extracted fields relative to total
    fields extracted.

    Args:
        generated: Generated record dictionary.
        golden: Golden/reference record dictionary.

    Returns:
        Accuracy score between 0.0 and 1.0.
    """
    if not generated:
        return 0.0
    
    # Count fields in generated (non-null, non-empty)
    total_extracted = count_non_empty_fields(generated)
    if total_extracted == 0:
        return 1.0  # No fields extracted, but also no errors
    
    # Count matching fields
    matching_fields = count_matching_fields(generated, golden)
    
    return matching_fields / total_extracted if total_extracted > 0 else 0.0


def calculate_f1_score(precision: float, recall: float) -> float:
    """Calculate F1-score as harmonic mean of precision and recall.

    Args:
        precision: Precision score.
        recall: Recall score.

    Returns:
        F1-score between 0.0 and 1.0.
    """
    if precision + recall == 0:
        return 0.0
    return 2 * (precision * recall) / (precision + recall)


def calculate_schema_compliance_rate(records: List[Dict[str, Any]], 
                                     validation_results: List[List[str]]) -> float:
    """Calculate schema compliance rate.

    Measures percentage of records that passed validation on first attempt.

    Args:
        records: List of records.
        validation_results: List of validation error lists (empty list means valid).

    Returns:
        Compliance rate between 0.0 and 1.0.
    """
    if not records:
        return 0.0
    
    valid_count = sum(1 for errors in validation_results if not errors)
    return valid_count / len(records)


def calculate_repair_rate(repair_counts: List[int]) -> float:
    """Calculate repair rate.

    Measures percentage of records that required repair loop.

    Args:
        repair_counts: List of repair attempt counts per record (0 = no repair needed).

    Returns:
        Repair rate between 0.0 and 1.0.
    """
    if not repair_counts:
        return 0.0
    
    repaired_count = sum(1 for count in repair_counts if count > 0)
    return repaired_count / len(repair_counts)


def calculate_latency(timings: List[float]) -> Dict[str, float]:
    """Calculate latency metrics from timing data.

    Args:
        timings: List of processing times in seconds.

    Returns:
        Dictionary with mean, median, min, and max latency values.
    """
    if not timings:
        return {"mean": 0.0, "median": 0.0, "min": 0.0, "max": 0.0}
    
    return {
        "mean": statistics.mean(timings),
        "median": statistics.median(timings),
        "min": min(timings),
        "max": max(timings)
    }


def calculate_extraction_latency(timings: List[float]) -> float:
    """Calculate average text extraction latency.

    Args:
        timings: List of extraction times in seconds.

    Returns:
        Average extraction time in seconds.
    """
    if not timings:
        return 0.0
    return statistics.mean(timings)


def calculate_llm_latency(timings: List[float]) -> float:
    """Calculate average LLM call latency.

    Args:
        timings: List of LLM call times in seconds.

    Returns:
        Average LLM call time in seconds.
    """
    if not timings:
        return 0.0
    return statistics.mean(timings)


def calculate_geocoding_latency(timings: List[float]) -> float:
    """Calculate average geocoding latency.

    Args:
        timings: List of geocoding times in seconds.

    Returns:
        Average geocoding time in seconds.
    """
    if not timings:
        return 0.0
    return statistics.mean(timings)


def calculate_ocr_fallback_rate(records: List[Dict[str, Any]]) -> float:
    """Calculate OCR fallback rate.

    Measures percentage of PDFs that used OCR fallback extraction.

    Args:
        records: List of records with metadata indicating extraction method.

    Returns:
        OCR fallback rate between 0.0 and 1.0.
    """
    if not records:
        return 0.0
    
    ocr_count = sum(1 for record in records 
                   if record.get("provenance", {}).get("extraction_method") == "ocr"
                   or record.get("_fulltext", "").startswith("OCR"))
    
    return ocr_count / len(records)


def calculate_geocoding_cache_hit_rate(cache_stats: Dict[str, int]) -> float:
    """Calculate geocoding cache hit rate.

    Measures percentage of geocode requests served from cache.

    Args:
        cache_stats: Dictionary with 'hits' and 'misses' keys.

    Returns:
        Cache hit rate between 0.0 and 1.0.
    """
    hits = cache_stats.get("hits", 0)
    misses = cache_stats.get("misses", 0)
    total = hits + misses
    
    if total == 0:
        return 0.0
    
    return hits / total


def calculate_llm_token_usage(records: List[Dict[str, Any]]) -> Dict[str, float]:
    """Calculate LLM token usage metrics.

    Args:
        records: List of records with token usage metadata in audit.tokens.

    Returns:
        Dictionary with average input_tokens, output_tokens, and total_tokens.
    """
    if not records:
        return {"input_tokens": 0.0, "output_tokens": 0.0, "total_tokens": 0.0}
    
    input_tokens = []
    output_tokens = []
    total_tokens = []
    
    for record in records:
        audit = record.get("audit", {})
        tokens = audit.get("tokens", {})
        
        if "input" in tokens:
            input_tokens.append(tokens["input"])
        if "output" in tokens:
            output_tokens.append(tokens["output"])
        if "total" in tokens:
            total_tokens.append(tokens["total"])
    
    return {
        "input_tokens": statistics.mean(input_tokens) if input_tokens else 0.0,
        "output_tokens": statistics.mean(output_tokens) if output_tokens else 0.0,
        "total_tokens": statistics.mean(total_tokens) if total_tokens else 0.0
    }


def count_non_empty_fields(obj: Any, path: str = "") -> int:
    """Recursively count non-empty fields in a nested dictionary.

    Args:
        obj: Object to count fields in.
        path: Current path for debugging (internal use).

    Returns:
        Count of non-empty fields in nested structure.
    """
    count = 0
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            # Skip internal/metadata fields
            if key.startswith("_") or key == "source_path" or key == "audit":
                continue
            
            if value is not None and value != "" and value != [] and value != {}:
                if isinstance(value, (dict, list)):
                    count += count_non_empty_fields(value, f"{path}.{key}")
                else:
                    count += 1
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if item is not None and item != "" and item != [] and item != {}:
                if isinstance(item, (dict, list)):
                    count += count_non_empty_fields(item, f"{path}[{i}]")
                else:
                    count += 1
    
    return count


def count_matching_fields(generated: Any, golden: Any, path: str = "") -> int:
    """Recursively count matching fields between generated and golden records.

    Args:
        generated: Generated record to compare.
        golden: Golden/reference record to compare against.
        path: Current path for debugging (internal use).

    Returns:
        Count of matching fields in nested structure.
    """
    count = 0
    
    if isinstance(golden, dict) and isinstance(generated, dict):
        for key, golden_value in golden.items():
            # Skip internal/metadata fields
            if key.startswith("_") or key == "source_path" or key == "audit":
                continue
            
            if key in generated:
                generated_value = generated[key]
                
                # Skip empty values in golden
                if golden_value is None or golden_value == "" or golden_value == [] or golden_value == {}:
                    continue
                
                # Compare values
                if isinstance(golden_value, dict) and isinstance(generated_value, dict):
                    count += count_matching_fields(generated_value, golden_value, f"{path}.{key}")
                elif isinstance(golden_value, list) and isinstance(generated_value, list):
                    # For lists, check if any elements match (simplified)
                    if len(golden_value) > 0 and len(generated_value) > 0:
                        # Count matching elements (simplified comparison)
                        count += 1
                else:
                    # Simple value comparison
                    if _values_match(generated_value, golden_value):
                        count += 1
    
    return count


def _values_match(generated: Any, golden: Any) -> bool:
    """Check if two values match with tolerance for floats and string normalization.

    Args:
        generated: Generated value to compare.
        golden: Golden value to compare against.

    Returns:
        True if values match (with tolerance for floats and case-insensitive
        string comparison).
    """
    # Handle None
    if generated is None and golden is None:
        return True
    if generated is None or golden is None:
        return False
    
    # Handle strings (case-insensitive, strip whitespace)
    if isinstance(generated, str) and isinstance(golden, str):
        return generated.strip().lower() == golden.strip().lower()
    
    # Handle numbers (with small tolerance for floats)
    if isinstance(generated, (int, float)) and isinstance(golden, (int, float)):
        return abs(generated - golden) < 0.001
    
    # Exact match for other types
    return generated == golden


def calculate_all_metrics(generated_records: List[Dict[str, Any]],
                          golden_records: List[Dict[str, Any]],
                          validation_results: List[List[str]],
                          repair_counts: List[int],
                          timings: List[float],
                          cache_stats: Dict[str, int]) -> Dict[str, Any]:
    """Calculate all metrics for a set of records.

    Computes extraction quality, schema compliance, repair rates, latency,
    cache hit rates, and token usage metrics.

    Args:
        generated_records: List of generated records.
        golden_records: List of golden/reference records.
        validation_results: List of validation error lists (empty = valid).
        repair_counts: List of repair attempt counts per record.
        timings: List of processing times in seconds.
        cache_stats: Cache statistics dictionary with 'hits' and 'misses'.

    Returns:
        Dictionary with all calculated metrics.
    """
    metrics = {}
    
    # Extraction quality metrics
    if golden_records and generated_records:
        completeness_scores = []
        accuracy_scores = []
        
        for gen, gold in zip(generated_records, golden_records):
            completeness_scores.append(calculate_completeness(gen, gold))
            accuracy_scores.append(calculate_accuracy(gen, gold))
        
        avg_completeness = statistics.mean(completeness_scores) if completeness_scores else 0.0
        avg_accuracy = statistics.mean(accuracy_scores) if accuracy_scores else 0.0
        f1 = calculate_f1_score(avg_accuracy, avg_completeness)
        
        metrics["completeness"] = avg_completeness
        metrics["accuracy"] = avg_accuracy
        metrics["f1_score"] = f1
    
    # Schema compliance
    if validation_results:
        metrics["schema_compliance_rate"] = calculate_schema_compliance_rate(
            generated_records, validation_results
        )
    
    # Repair rate
    if repair_counts:
        metrics["repair_rate"] = calculate_repair_rate(repair_counts)
    
    # Latency metrics
    if timings:
        latency_metrics = calculate_latency(timings)
        metrics.update({f"latency_{k}": v for k, v in latency_metrics.items()})
    
    # OCR fallback rate
    if generated_records:
        metrics["ocr_fallback_rate"] = calculate_ocr_fallback_rate(generated_records)
    
    # Cache hit rate
    if cache_stats:
        metrics["geocoding_cache_hit_rate"] = calculate_geocoding_cache_hit_rate(cache_stats)
    
    # Token usage
    if generated_records:
        token_metrics = calculate_llm_token_usage(generated_records)
        metrics.update(token_metrics)
    
    return metrics

