import datetime
import json
import databox
import asyncio
import aiohttp
import pandas
from dateutil.relativedelta import relativedelta

TIME_BEGIN = datetime.datetime.now()
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', 2000)


#######################################################################################################################
#
# Class definition
#
#######################################################################################################################

class Picqer:
    """ Picqer class

    Attributes:
        <class> loop: Asyncio event loop
        <int> requests: Total number of API request made
        <dict> requestHeaders: HTTP Request Headers
        <class> requestTimeout: Asyncio ClientTimeout instance
        <str> requestURL: API Base URL
        <class> semaphore: Asyncio semaphore
        <str> token: Data Source token
    """

    def __init__(self, _semaphore_size_=5):
        """Initializes the instance

        Args:
            <int> _semaphore_size_: Asyncio semaphore size
        """

        self.accessToken = ''
        self.DataboxToken = ''
        self.requests = 0
        self.requestHeaders = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': self.accessToken
        }
        self.requestTimeout = aiohttp.ClientTimeout(total=None, sock_connect=240, sock_read=240)
        self.requestURL = 'https://alleeninktnl.picqer.com/api/v1'
        self.semaphore = asyncio.Semaphore(_semaphore_size_)

        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.exception)

    def exception(self, _loop_, _context_):
        """Handles exceptions raised during pushes

        Args:
            <class> _loop_: Asyncio event loop
            <exception> _context_: Exception raised

        Returns:
            <void>
        """

        self.loop.default_exception_handler(_context_)

        exception = _context_.get('exception')
        if isinstance(exception, Exception):
            print(_context_)
            self.loop.stop()

    async def fetch(self, _endpoint_, _params_, _body_):
        """Makes an API request to Picker API

        Args:
            <str> _token_: Data Source token
            <str> _endpoint_: API endpoint
            <dict> _params_: API request params
            <dict> _body_: API request body

        Returns:
            <dict>: Dictionary with API response data
        """

        client = aiohttp.ClientSession(headers=self.requestHeaders, timeout=self.requestTimeout)

        async with self.semaphore:
            async with client as session:
                url = self.requestURL + _endpoint_

                print('Picker >', url, '>', json.dumps(_params_))

                async with session.get(url=url, json=_body_, params=_params_) as response:
                    result = {
                        'endpoint': _endpoint_,
                        'params': _params_,
                        'body': _body_,
                        'code': response.status,
                        'reason': response.reason,
                        'text': json.loads(await response.text())
                    }
                    self.requests += 1
                    return result

    def create_count_metric(self, pandasdataframe, date_column, value_column, metric_name, dimension=None):
        """From a Pandas DataFrame create a dictionary in the format required by Databox API for a Metric
        Args:
            <pandas.DataFrame> pandasdataframe: DataFrame to create the metric from
            <str> date_column: Name of the DataFrame column to be used as date
            <str> value_column: Name of the DataFrame column to be used as value
            <str> metric_name: the metric name to be sent to databox
            <str> dimension: the name of the DataFrame column to be used as dimension
        """
        if dimension is not None:
            new_metric = pandasdataframe.groupby([date_column, dimension])[value_column].nunique()\
                .reset_index().to_dict(orient="index")
        else:
            new_metric = pandasdataframe.groupby([date_column])[value_column].nunique() \
                .reset_index().to_dict(orient="index")

        for index, row in new_metric.items():
            data = {
                '$' + metric_name: row[value_column],
                'date': row[date_column],
            }
            if dimension is not None:
                data[dimension.title()] = row[dimension]

            databox.append(self.DataboxToken, data)

    def create_sum_metric(self, pandasdataframe, date_column, value_column, metric_name, dimension=None):
        """From a Pandas DataFrame create a dictionary in the format required by Databox API for a Metric
        Args:
            <pandas.DataFrame> pandasdataframe: DataFrame to create the metric from
            <str> date_column: Name of the DataFrame column to be used as date
            <str> value_column: Name of the DataFrame column to be used as value
            <str> metric_name: the metric name to be sent to databox
            <str> dimension: the name of the DataFrame column to be used as dimension
        """

        if dimension is None:
            new_metric = pandasdataframe.groupby([date_column])[value_column].sum()\
                .reset_index().to_dict(orient="index")
        else:
            new_metric = pandasdataframe.groupby([date_column, dimension])[value_column].sum()\
                .reset_index().to_dict(orient="index")

        for index, row in new_metric.items():
            data = {
                '$' + metric_name.title(): row[value_column],
                'date': row[date_column]
            }
            if dimension is not None:
                data[dimension] = row[dimension]

            databox.append(self.DataboxToken, data)

    async def picklist(self, _monthoffsets_):

        """Fetches all relevant orders from the picklist data
        Args:
            <int> _campaign_id_: id of the corresponding campaign

        Returns:
            <list>: Dictionary with datapoints
        """

        endpoint = '/picklists'
        body = {}
        data = []
        params = {
            'page': 1,
            'offset': 0,
            'sincedate': (datetime.datetime.today().replace(day=1) -
                          relativedelta(months=_monthoffsets_)).strftime('%Y-%m-%d'),
            'untildate': datetime.datetime.today().strftime('%Y-%m-%d')
        }

        while True:
            result = await self.fetch(endpoint, params, body)
            picklists_data = result['text']

            try:
                for picklists in picklists_data:
                    data.append({
                        'id': picklists['idorder'],
                        'status': picklists['status'],
                        'created_at': picklists['created'],
                        'id_user': picklists['closed_by_iduser'],
                        'product_amount': picklists['totalproducts']
                    })

            except KeyError:
                print(picklists_data)

            if len(picklists_data) > 0:
                await asyncio.sleep(0.5)
                params['offset'] += 100
            else:
                break

        return data

    async def users(self):

        """Fetches all relevant users data
        Args:
            <int> _campaign_id_: id of the corresponding campaign

        Returns:
            <list>: Dictionary with datapoints
        """

        endpoint = '/users'
        body = {}
        data = []
        params = {
            'page': 1,
            'offset': 0
        }

        while True:
            result = await self.fetch(endpoint, params, body)
            users_data = result['text']

            for users in users_data:
                data.append({
                    'id_user': users['iduser'],
                    'name': users['firstname'] + ' ' + users['lastname']
                })

            if len(users_data) > 0:
                await asyncio.sleep(0.5)
                params['offset'] += 100
            else:
                break

        return data


#######################################################################################################################
#
# Define Metrics
#
#######################################################################################################################

async def main():
    picqer = Picqer()
    
    # Create Pandas DataFrames from Picqer API response
    users_df = pandas.DataFrame(await picqer.users())
    orders_df = pandas.DataFrame(await picqer.picklist(_monthoffsets_=0)).merge(right=users_df, on='id_user')

    # Define metrics to be push to Databox
    picqer.create_count_metric(pandasdataframe=orders_df, value_column='id', date_column='created_at',
                               dimension='name', metric_name='Orders')
    picqer.create_sum_metric(pandasdataframe=orders_df, value_column='product_amount', date_column='created_at',
                             dimension='name', metric_name='Product_Amount')


#######################################################################################################################
#
# Execute
#
#######################################################################################################################

databox = databox.Databox()

asyncio.run(main())

databox.push()

print(datetime.datetime.now() - TIME_BEGIN)
