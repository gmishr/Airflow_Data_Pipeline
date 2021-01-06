from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        if len(self.dq_checks) == 0:
            self.log.info('No Data Quality Check Provided.')
            return
        err_cnt = 0
        failed_test = []
        for check in self.dq_checks:
            sql_query = check.get("sql_query")
            expected_result = check.get("expected_result")
            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
            try:
                self.log.info('{} is executing'.format(sql_query))
                output = redshift.get_records(sql_query)[0]
                
            except Exception as e:
                self.log.info('{} query failed with error {}'.format(sql_query,e))
            
            if expected_result != output[0]:
                err_cnt+=1
                failed_test.append(sql_query)
         
        if err_cnt > 0:
            self.log.info('{} Data Checks Failed'.format(err_cnt))
            self.log.info(failed_test)
            raise ValueError('Data Quality Checks Failed.')
        else:
            self.log.info('All data quality checks are passed.')
