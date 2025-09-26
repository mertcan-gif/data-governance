from datahub.emitter.mce_builder import make_dataset_urn, make_glossary_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import GlossaryTermAssociationClass, GlossaryTermsClass

# --- Configuration ---
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
dataset_urn = make_dataset_urn("postgres", "hr_db.employees", "PROD")

# --- Define the Glossary Term URN ---
# This assumes a term named 'Personally Identifiable Information' already exists in your glossary.
pii_term_urn = make_glossary_term_urn("Personally Identifiable Information")

# --- Create the association aspect ---
glossary_term_aspect = GlossaryTermsClass(
    terms=[
        GlossaryTermAssociationClass(urn=pii_term_urn)
    ]
)

# --- Emit the MCP to link the term to the employees table ---
mcp_glossary = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=glossary_term_aspect,
)
emitter.emit_mcp(mcp_glossary)
print(f"Successfully associated glossary term '{pii_term_urn}' with '{dataset_urn}'")