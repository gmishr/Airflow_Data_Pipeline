from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_dimension_table = """
                           INSERT INTO {}
                           {}
                           """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 mode="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.mode = mode
        self.sql_stmt = sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.mode == "append":
            formatted_sql = LoadDimensionOperator.load_dimension_table.format(
            self.table,
            self.sql_stmt
            )
            redshift.run(formatted_sql)
        elif self.mode == "delete":
            redshift.run(f"DELETE FROM {self.table};")
            formatted_sql = LoadDimensionOperator.load_dimension_table.format(
            self.table,
            self.sql_stmt
            )
            redshift.run(formatted_sql)
            
        else:
            redshift.run(f"DELETE FROM {self.table};")
            formatted_sql = LoadDimensionOperator.load_dimension_table.format(
            self.table,
            self.sql_stmt
            )
            redshift.run(formatted_sql)
        
        self.log.info(f"Table {self.table} data load completed")
