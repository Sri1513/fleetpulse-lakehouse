SILVER_TABLE_CONFIG = {
    "branches": {
        "table_name" : "branches",
        "primary_key" : ["branch_id"],
        "trim_columns" : [
            "branch_name",
            "region",
            "district",
            "city",
            "postal_code",
            "source_table"
        ],
        "uppercase_columns" : [
            "branch_code",
            "state",
            "country",
            "branch_status",
            "source_system",
            "bronze_load_type"
        ],
        "defaults": {
            "country": "USA",
            "branch_status": "UNKNOWN",
            "is_deleted": False
        },
        "hash_columns": [
            "branch_code",
            "branch_name",
            "region",
            "district",
            "city",
            "state",
            "country",
            "postal_code",
            "latitude",
            "longitude",
            "branch_status",
            "is_deleted"
        ],
        "required_columns": [
            "branch_id",
            "branch_code",
            "branch_name"
        ],
        "order_columns":[
            "updated_at",
            "ingestion_timestamp"
        ]

    },
    "customers": {
    "table_name": "customers",
    "primary_key": ["customer_id"],
    "trim_columns": [
        "customer_name",
        "industry",
        "contact_email",
        "contact_phone",
        "billing_city",
        "source_table"
    ],

    "uppercase_columns": [
        "customer_number",
        "customer_type",
        "billing_state",
        "billing_country",
        "credit_status",
        "customer_status",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "billing_country": "USA",
        "credit_status": "UNKNOWN",
        "customer_status": "UNKNOWN",
        "is_deleted": False
    },

    "hash_columns": [
        "customer_number",
        "customer_name",
        "customer_type",
        "industry",
        "billing_city",
        "billing_state",
        "billing_country",
        "credit_status",
        "customer_status",
        "is_deleted"
    ],

    "required_columns": [
        "customer_id",
        "customer_number",
        "customer_name"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ]
},
"equipment_categories": {
    "table_name": "equipment_categories",
    "primary_key": ["category_id"],

    "trim_columns": [
        "category_name",
        "equipment_class",
        "equipment_type",
        "source_table"
    ],

    "uppercase_columns": [
        "category_code",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "is_deleted": False
    },

    "hash_columns": [
        "category_code",
        "category_name",
        "parent_category_id",
        "equipment_class",
        "equipment_type",
        "is_deleted"
    ],

    "required_columns": [
        "category_id",
        "category_code",
        "category_name"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ]
},
"equipment": {
    "table_name": "equipment",
    "primary_key": ["equipment_id"],

    "trim_columns": [
        "manufacturer",
        "model",
        "source_table"
    ],

    "uppercase_columns": [
        "equipment_number",
        "serial_number",
        "equipment_status",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "equipment_status": "UNKNOWN",
        "telematics_enabled": False,
        "is_deleted": False
    },

    "hash_columns": [
        "equipment_number",
        "serial_number",
        "category_id",
        "owning_branch_id",
        "current_branch_id",
        "manufacturer",
        "model",
        "model_year",
        "acquisition_date",
        "acquisition_cost",
        "equipment_status",
        "telematics_enabled",
        "is_deleted"
    ],

    "required_columns": [
        "equipment_id",
        "equipment_number",
        "category_id",
        "owning_branch_id",
        "current_branch_id"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ]
},
"rental_contracts": {
    "table_name": "rental_contracts",
    "primary_key": ["contract_id"],

    "trim_columns": [
        "contract_number",
        "payment_terms",
        "source_table"
    ],

    "uppercase_columns": [
        "contract_status",
        "sales_channel",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "contract_status": "UNKNOWN",
        "sales_channel": "UNKNOWN",
        "discount_amount": 0.00,
        "tax_amount": 0.00,
        "subtotal_amount": 0.00,
        "total_amount": 0.00,
        "is_deleted": False
    },

    "hash_columns": [
        "contract_number",
        "customer_id",
        "branch_id",
        "contract_created_date",
        "rental_start_date",
        "expected_return_date",
        "actual_return_date",
        "contract_status",
        "sales_channel",
        "payment_terms",
        "subtotal_amount",
        "discount_amount",
        "tax_amount",
        "total_amount",
        "is_deleted"
    ],

    "required_columns": [
        "contract_id",
        "contract_number",
        "customer_id",
        "branch_id",
        "contract_created_date",
        "rental_start_date"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ],

    "referential_checks": [
        {
            "parent_table": "customers",
            "child_key": "customer_id",
            "parent_key": "customer_id"
        },
        {
            "parent_table": "branches",
            "child_key": "branch_id",
            "parent_key": "branch_id"
        }
    ]
},
"rental_contract_lines": {
    "table_name": "rental_contract_lines",
    "primary_key": ["contract_line_id"],

    "trim_columns": [
        "line_status",
        "source_table"
    ],

    "uppercase_columns": [
        "line_status",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "rental_quantity": 1,
        "daily_rate": 0.00,
        "weekly_rate": 0.00,
        "monthly_rate": 0.00,
        "charged_rate": 0.00,
        "estimated_days": 0,
        "actual_days": 0,
        "line_subtotal_amount": 0.00,
        "line_discount_amount": 0.00,
        "line_tax_amount": 0.00,
        "line_total_amount": 0.00,
        "line_status": "UNKNOWN",
        "is_deleted": False
    },

    "hash_columns": [
        "contract_id",
        "equipment_id",
        "rental_quantity",
        "daily_rate",
        "weekly_rate",
        "monthly_rate",
        "charged_rate",
        "estimated_days",
        "actual_days",
        "line_subtotal_amount",
        "line_discount_amount",
        "line_tax_amount",
        "line_total_amount",
        "line_status",
        "is_deleted"
    ],

    "required_columns": [
        "contract_line_id",
        "contract_id",
        "equipment_id"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ],

    "referential_checks": [
        {
            "parent_table": "rental_contracts",
            "child_key": "contract_id",
            "parent_key": "contract_id"
        },
        {
            "parent_table": "equipment",
            "child_key": "equipment_id",
            "parent_key": "equipment_id"
        }
    ]
},
"invoices": {
    "table_name": "invoices",
    "primary_key": ["invoice_id"],

    "trim_columns": [
        "invoice_number",
        "invoice_status",
        "source_table"
    ],

    "uppercase_columns": [
        "invoice_number",
        "invoice_status",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "invoice_status": "UNKNOWN",
        "subtotal_amount": 0.00,
        "discount_amount": 0.00,
        "tax_amount": 0.00,
        "total_amount": 0.00,
        "balance_due_amount": 0.00,
        "is_deleted": False
    },

    "hash_columns": [
        "invoice_number",
        "contract_id",
        "customer_id",
        "branch_id",
        "invoice_date",
        "due_date",
        "invoice_status",
        "subtotal_amount",
        "discount_amount",
        "tax_amount",
        "total_amount",
        "balance_due_amount",
        "is_deleted"
    ],

    "required_columns": [
        "invoice_id",
        "invoice_number",
        "contract_id",
        "customer_id",
        "branch_id",
        "invoice_date"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ],

    "referential_checks": [
        {
            "parent_table": "rental_contracts",
            "child_key": "contract_id",
            "parent_key": "contract_id"
        },
        {
            "parent_table": "customers",
            "child_key": "customer_id",
            "parent_key": "customer_id"
        },
        {
            "parent_table": "branches",
            "child_key": "branch_id",
            "parent_key": "branch_id"
        }
    ]
},
"invoice_lines": {
    "table_name": "invoice_lines",
    "primary_key": ["invoice_line_id"],

    "trim_columns": [
        "line_description",
        "revenue_type",
        "source_table"
    ],

    "uppercase_columns": [
        "revenue_type",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "revenue_type": "UNKNOWN",
        "quantity": 0.00,
        "unit_price": 0.00,
        "line_subtotal_amount": 0.00,
        "line_discount_amount": 0.00,
        "line_tax_amount": 0.00,
        "line_total_amount": 0.00,
        "is_deleted": False
    },

    "hash_columns": [
        "invoice_id",
        "contract_line_id",
        "line_description",
        "revenue_type",
        "quantity",
        "unit_price",
        "line_subtotal_amount",
        "line_discount_amount",
        "line_tax_amount",
        "line_total_amount",
        "is_deleted"
    ],

    "required_columns": [
        "invoice_line_id",
        "invoice_id",
        "line_description",
        "revenue_type"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ],

    "referential_checks": [
        {
            "parent_table": "invoices",
            "child_key": "invoice_id",
            "parent_key": "invoice_id"
        },
        {
            "parent_table": "rental_contract_lines",
            "child_key": "contract_line_id",
            "parent_key": "contract_line_id"
        }
    ]
},
"payments": {
    "table_name": "payments",
    "primary_key": ["payment_id"],

    "trim_columns": [
        "payment_number",
        "payment_method",
        "payment_status",
        "transaction_reference",
        "source_table"
    ],

    "uppercase_columns": [
        "payment_number",
        "payment_method",
        "payment_status",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "payment_method": "UNKNOWN",
        "payment_status": "UNKNOWN",
        "payment_amount": 0.00,
        "is_deleted": False
    },

    "hash_columns": [
        "payment_number",
        "invoice_id",
        "customer_id",
        "payment_date",
        "payment_method",
        "payment_status",
        "payment_amount",
        "transaction_reference",
        "is_deleted"
    ],

    "required_columns": [
        "payment_id",
        "payment_number",
        "invoice_id",
        "customer_id",
        "payment_date"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ],

    "referential_checks": [
        {
            "parent_table": "invoices",
            "child_key": "invoice_id",
            "parent_key": "invoice_id"
        },
        {
            "parent_table": "customers",
            "child_key": "customer_id",
            "parent_key": "customer_id"
        }
    ]
},
"maintenance_work_orders": {
    "table_name": "maintenance_work_orders",
    "primary_key": ["work_order_id"],

    "trim_columns": [
        "work_order_number",
        "work_order_type",
        "priority",
        "work_order_status",
        "issue_description",
        "resolution_description",
        "source_table"
    ],

    "uppercase_columns": [
        "work_order_number",
        "work_order_type",
        "priority",
        "work_order_status",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "work_order_type": "UNKNOWN",
        "priority": "UNKNOWN",
        "work_order_status": "UNKNOWN",
        "labor_hours": 0.00,
        "parts_cost": 0.00,
        "labor_cost": 0.00,
        "total_maintenance_cost": 0.00,
        "is_deleted": False
    },

    "hash_columns": [
        "work_order_number",
        "equipment_id",
        "branch_id",
        "work_order_type",
        "priority",
        "work_order_status",
        "opened_date",
        "scheduled_date",
        "completed_date",
        "issue_description",
        "resolution_description",
        "labor_hours",
        "parts_cost",
        "labor_cost",
        "total_maintenance_cost",
        "is_deleted"
    ],

    "required_columns": [
        "work_order_id",
        "work_order_number",
        "equipment_id",
        "branch_id",
        "opened_date"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ],

    "referential_checks": [
        {
            "parent_table": "equipment",
            "child_key": "equipment_id",
            "parent_key": "equipment_id"
        },
        {
            "parent_table": "branches",
            "child_key": "branch_id",
            "parent_key": "branch_id"
        }
    ]
},
"equipment_status_history": {
    "table_name": "equipment_status_history",
    "primary_key": ["status_history_id"],

    "trim_columns": [
        "old_status",
        "new_status",
        "status_change_reason",
        "source_table"
    ],

    "uppercase_columns": [
        "old_status",
        "new_status",
        "source_system",
        "bronze_load_type"
    ],

    "defaults": {
        "old_status": "UNKNOWN",
        "new_status": "UNKNOWN",
        "status_change_reason": "UNKNOWN",
        "is_deleted": False
    },

    "hash_columns": [
        "equipment_id",
        "branch_id",
        "old_status",
        "new_status",
        "status_change_reason",
        "status_start_timestamp",
        "status_end_timestamp",
        "is_deleted"
    ],

    "required_columns": [
        "status_history_id",
        "equipment_id",
        "branch_id",
        "new_status",
        "status_start_timestamp"
    ],

    "order_columns": [
        "updated_at",
        "ingestion_timestamp"
    ],

    "referential_checks": [
        {
            "parent_table": "equipment",
            "child_key": "equipment_id",
            "parent_key": "equipment_id"
        },
        {
            "parent_table": "branches",
            "child_key": "branch_id",
            "parent_key": "branch_id"
        }
    ]
}
}
