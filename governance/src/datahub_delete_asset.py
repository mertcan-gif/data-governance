from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import StatusClass

# --- Configuration ---
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
dataset_to_delete_urn = make_dataset_urn("postgres", "hr_db.old_employees_temp", "PROD")

# --- Create the status aspect for soft deletion ---
soft_delete_aspect = StatusClass(removed=True)

# --- Emit the MCP to soft delete the asset ---
mcp_delete = MetadataChangeProposalWrapper(
    entityUrn=dataset_to_delete_urn,
    aspect=soft_delete_aspect,
)
emitter.emit_mcp(mcp_delete)
print(f"Successfully soft-deleted asset: {dataset_to_delete_urn}")