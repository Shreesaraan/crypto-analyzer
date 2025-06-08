# Azure ADLS Folder Structure

Container: `crypto-data`

├── raw/
│   └── {yyyy-MM-dd}/
│       └── crypto_{timestamp}.json
├── processed/
│   └── {yyyy-MM-dd}.parquet
└── output/
    ├── daily_summary/
    │   └── {yyyy-MM-dd}_summary.parquet
    └── overall_summary/
        └── {yyyy-MM-dd}_overall_summary.parquet
