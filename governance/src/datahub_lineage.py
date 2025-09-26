from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import UpstreamClass, UpstreamLineageClass

# --- Configuration ---
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
platform = "postgres"
environment = "PROD"
db_name = "hr_db"

# --- Define URNs for source and destination tables ---
source_table_urn = make_dataset_urn(platform, f"{db_name}.employees", environment)
destination_table_urn = make_dataset_urn(platform, f"{db_name}.daily_employee_report", environment)

# --- Create the lineage relationship ---
lineage_aspect = UpstreamLineageClass(
    upstreams=[
        UpstreamClass(
            dataset=source_table_urn,
            type="TRANSFORMED", # Other types include COPY, VIEW, etc.
        )
    ]
)

# --- Emit the lineage MCP ---
mcp_lineage = MetadataChangeProposalWrapper(
    entityUrn=destination_table_urn,
    aspect=lineage_aspect,
)
emitter.emit_mcp(mcp_lineage)
print(f"Successfully emitted lineage: {source_table_urn} -> {destination_table_urn}")
