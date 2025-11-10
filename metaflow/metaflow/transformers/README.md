# MetaFlow Transformers

Data transformation utilities for cleaning, validating, and processing data.

## Available Transformers

### BaseTransformer
Abstract base class for all transformers with common functionality.

### DataQualityTransformer
Validate and cleanse data based on quality rules.

**Example:**
```python
from metaflow.transformers import DataQualityTransformer, QualityRule, RuleType, TransformerConfig

config = TransformerConfig(enable_metrics=True)

rules = [
    QualityRule(
        name="email_not_null",
        column="email",
        rule_type=RuleType.NOT_NULL,
        parameters={},
        severity="error"
    ),
    QualityRule(
        name="age_range",
        column="age",
        rule_type=RuleType.RANGE,
        parameters={'min': 0, 'max': 120},
        severity="warning"
    ),
    QualityRule(
        name="email_pattern",
        column="email",
        rule_type=RuleType.PATTERN,
        parameters={'pattern': r'^[\w\.-]+@[\w\.-]+\.\w+$'},
        severity="error"
    )
]

transformer = DataQualityTransformer(
    spark,
    config,
    rules=rules,
    quarantine_invalid=True,
    quarantine_path='/data/quarantine'
)

cleaned_df = transformer.transform(df)
metrics = transformer.get_quality_metrics()
print(f"Quality score: {metrics['quality_score']:.2f}%")