from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    load_fact_table = """
                     INSERT INTO {}
                     {}
                     """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift.run(f"DELETE FROM {self.table};")
        formatted_sql = LoadFactOperator.load_fact_table.format(
             self.table,
             self.sql_stmt
        )
        redshift.run(formatted_sql)
        self.log.info(f"Table {self.table} load completed.")
