"""update datasources

Revision ID: 937d04c16b64
Revises: 1d9e835a84f9
Create Date: 2018-07-20 16:08:10.195843

"""

# revision identifiers, used by Alembic.
revision = '937d04c16b64'
down_revision = '1d9e835a84f9'

from alembic import op
import sqlalchemy as sa


datasources = sa.Table(
    'datasources',
    sa.MetaData(),
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('datasource_name', sa.String(255), nullable=False),
)


def upgrade():
    bind = op.get_bind()
    insp = sa.engine.reflection.Inspector.from_engine(bind)

    # Enforce that the datasource_name column be non-nullable.
    with op.batch_alter_table('datasources', schema=None) as batch_op:
        batch_op.alter_column(
            'datasource_name',
            nullable=False,
            type_=sa.String(255),
        )


def downgrade():
    bind = op.get_bind()
    insp = sa.engine.reflection.Inspector.from_engine(bind)

    # Forego that the datasource_name column be non-nullable.
    with op.batch_alter_table('datasources', schema=None) as batch_op:
        batch_op.alter_column(
            'datasource_name',
            nullable=True,
            type_=sa.String(255),
        )
