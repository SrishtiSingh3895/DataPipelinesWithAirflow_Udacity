from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_queries="",
                 expected_results="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_queries = test_queries
        self.expected_results = expected_results

    def execute(self, context):
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Running data quality check")
        for i, test_query in enumerate(self.test_queries):
            records = redshift_hook.get_records(test_query)
            if records[0][0] != self.expected_results[i]:
                raise ValueError(f"""Data quality check failed! \
                    {records[0][0]} does not equal {self.expected_results[i]}
                """)
            else:
                self.log.info("Data quality check passed!")
        self.log.info("Data quality checks passed!")