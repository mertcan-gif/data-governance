from datahub.emitter.mce_builder import make_dataset_urn, make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DomainsClass

# --- Configuration ---
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
dataset_urn = make_dataset_urn("postgres", "hr_db.employees", "PROD")

# --- Define the Domain URN ---
# Assumes a 'Human Resources' domain exists.
hr_domain_urn = make_domain_urn("human_resources")

# --- Create the association aspect ---
domain_aspect = DomainsClass(
    domains=[hr_domain_urn]
)

# --- Emit the MCP to place the dataset in the domain ---
mcp_domain = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=domain_aspect,
)
emitter.emit_mcp(mcp_domain)
print(f"Successfully added '{dataset_urn}' to the '{hr_domain_urn}' domain.")