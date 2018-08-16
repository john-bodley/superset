"""unmaterialize perms

Revision ID: 89bc23ba804e
Revises: 1a1d627ebd8e
Create Date: 2018-08-16 09:41:57.787472

"""

# revision identifiers, used by Alembic.
revision = '89bc23ba804e'
down_revision = '1a1d627ebd8e'

from alembic import op
import sqlalchemy as sa


def upgrade():
    tables = [
        'datasources',
        'dbs',
        'slices',
        'tables',
    ]

    for table in tables:
        with op.batch_alter_table(table) as batch_op:
            batch_op.drop_column('perm')


def downgrade():
    pass
