import time
from datahub.emitter.mce_builder import make_dataset_urn, make_corp_user_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    EditableSchemaMetadataClass,
    EditableSchemaFieldInfoClass,
    GlobalTagsClass,
    OwnershipClass,
    OwnershipTypeClass,
    OwnerClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    NumberTypeClass,
    TagAssociationClass,
)

# --- 1. Configuration ---

# This is the URL to your DataHub GMS (General Metadata Service) instance.
# If you're running DataHub locally using the quickstart guide, it will be http://localhost:8080.
gms_server = "http://localhost:8080"

# Create an emitter to send metadata to DataHub REST API.
emitter = DatahubRestEmitter(gms_server=gms_server)

# --- 2. Define HR Metadata ---

# Define the platform and environment for your data assets.
platform = "postgres"
environment = "PROD"
db_name = "hr_db"

# Define owners for the data assets.
data_owner_urn = make_corp_user_urn("data-steward")
technical_owner_urn = make_corp_user_urn("data-engineer")

# Define tags for categorization.
pii_tag_urn = make_tag_urn("pii")
hr_tag_urn = make_tag_urn("human-resources")


def create_hr_metadata():
    """
    This function creates and emits metadata for the HR tables.
    """

    # --- 3. Employees Table Metadata ---

    table_name_employees = "employees"
    dataset_urn_employees = make_dataset_urn(platform, f"{db_name}.{table_name_employees}", environment)

    # Define the schema for the 'employees' table.
    schema_metadata_employees = SchemaMetadataClass(
        schemaName=table_name_employees,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=None,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="INTEGER",
                description="Unique identifier for each employee.",
            ),
            SchemaFieldClass(
                fieldPath="full_name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                description="The full name of the employee.",
            ),
            SchemaFieldClass(
                fieldPath="email",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                description="The employee's email address.",
                globalTags=GlobalTagsClass(
                    tags=[TagAssociationClass(tag=pii_tag_urn)]
                ),
            ),
            SchemaFieldClass(
                fieldPath="department_id",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="INTEGER",
                description="Foreign key to the departments table.",
            ),
        ],
    )

    # Create a MetadataChangeProposalWrapper for the schema.
    mcp_schema_employees = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn_employees,
        aspect=schema_metadata_employees,
    )
    emitter.emit_mcp(mcp_schema_employees)
    print(f"Emitted schema for {dataset_urn_employees}")

    # --- 4. Departments Table Metadata ---

    table_name_departments = "departments"
    dataset_urn_departments = make_dataset_urn(platform, f"{db_name}.{table_name_departments}", environment)

    # Define the schema for the 'departments' table.
    schema_metadata_departments = SchemaMetadataClass(
        schemaName=table_name_departments,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=None,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="INTEGER",
                description="Unique identifier for each department.",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                description="The name of the department.",
            ),
            SchemaFieldClass(
                fieldPath="description",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="TEXT",
                description="A brief description of the department's function.",
            ),
        ],
    )

    # Create a MetadataChangeProposalWrapper for the schema.
    mcp_schema_departments = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn_departments,
        aspect=schema_metadata_departments,
    )
    emitter.emit_mcp(mcp_schema_departments)
    print(f"Emitted schema for {dataset_urn_departments}")

    # --- 5. Add Ownership and Tags to both tables ---

    for urn in [dataset_urn_employees, dataset_urn_departments]:
        # Add ownership information.
        ownership_aspect = OwnershipClass(
            owners=[
                OwnerClass(owner=data_owner_urn, type=OwnershipTypeClass.DATAOWNER),
                OwnerClass(owner=technical_owner_urn, type=OwnershipTypeClass.TECHNICAL_OWNER),
            ]
        )
        mcp_ownership = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ownership_aspect,
        )
        emitter.emit_mode(mcp_ownership)
        print(f"Emitted ownership for {urn}")

        # Add global tags.
        tags_aspect = GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=hr_tag_urn),
            ]
        )
        mcp_tags = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=tags_aspect,
        )
        emitter.emit_mcp(mcp_tags)
        print(f"Emitted tags for {urn}")


if __name__ == "__main__":
    create_hr_metadata()
    print("Successfully emitted HR metadata to DataHub!")