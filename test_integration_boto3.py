from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
import threading

import urllib3.connection


from alternator_lb import AlternatorLB, Config


class TestAlternatorBotocore:
    initial_nodes = ['172.43.0.2']
    http_port = 9998
    https_port = 9999

    def test_check_if_rack_datacenter_feature_is_supported(self):
        lb = AlternatorLB(Config(nodes=self.initial_nodes,
                          port=self.http_port, datacenter="fake_dc"))
        lb.check_if_rack_datacenter_feature_is_supported()

    def test_check_if_rack_and_datacenter_set_correctly_fake_dc(self):
        lb = AlternatorLB(Config(nodes=self.initial_nodes,
                          port=self.http_port, datacenter="fake_dc"))
        try:
            lb.check_if_rack_and_datacenter_set_correctly()
            assert False, "Expected ValueError"
        except ValueError:
            pass

    def test_http_connection_persistent(self):
        self._test_connection_persistent("http", 1)
        self._test_connection_persistent("http", 2)

    def test_https_connection_persistent(self):
        self._test_connection_persistent("https", 1)
        self._test_connection_persistent("https", 2)

    def _test_connection_persistent(self, schema: str, max_pool_connections: int):
        cnt = 0
        if schema == "http":
            original_init = urllib3.connection.HTTPConnection.__init__
        else:
            original_init = urllib3.connection.HTTPSConnection.__init__

        def wrapper(self, *args, **kwargs):
            nonlocal cnt
            nonlocal original_init
            cnt += 1
            return original_init(self, *args, **kwargs)

        if schema == "http":
            patched = patch.object(
                urllib3.connection.HTTPConnection, '__init__', new=wrapper)
        else:
            patched = patch.object(
                urllib3.connection.HTTPSConnection, '__init__', new=wrapper)

        with patched:
            lb = AlternatorLB(Config(
                schema=schema,
                nodes=self.initial_nodes,
                port=self.http_port if schema == "http" else self.https_port,
                datacenter="fake_dc",
                update_interval=0,
                max_pool_connections=max_pool_connections,
            ))

            dynamodb = lb.new_boto3_dynamodb_client()
            try:
                dynamodb.delete_table(TableName="FakeTable")
            except Exception as e:
                if e.__class__.__name__ != "ResourceNotFoundException":
                    raise
            assert cnt == 1
            try:
                dynamodb.delete_table(TableName="FakeTable")
            except Exception as e:
                if e.__class__.__name__ != "ResourceNotFoundException":
                    raise
            assert cnt == 1  # Connection should be carried over to another request

            lb._update_live_nodes()
            assert cnt == 2  # AlternatorLB uses different connection pool, so one more connection will be created
            lb._update_live_nodes()
            assert cnt == 2  # And it should be carried over to another attempt of pulling nodes

    def test_connection_pool_with_concurrent_requests(self):
        """Test that connection pool properly handles concurrent requests."""
        self._test_concurrent_connection_reuse("http")
        self._test_concurrent_connection_reuse("https")

    def _test_concurrent_connection_reuse(self, schema: str):
        """
        Test connection reuse under concurrent load.

        This test tracks both:
        1. Number of times connections are created (constructor calls)
        2. Number of currently active/open connections

        We verify that after executing batch of requests, connections are
        reused and the pool stabilizes at the configured pool_size.
        """
        connections_created = 0
        active_connections = set()
        lock = threading.Lock()

        if schema == "http":
            original_init = urllib3.connection.HTTPConnection.__init__
            original_close = urllib3.connection.HTTPConnection.close
        else:
            original_init = urllib3.connection.HTTPSConnection.__init__
            original_close = urllib3.connection.HTTPSConnection.close

        def init_wrapper(self, *args, **kwargs):
            nonlocal connections_created
            with lock:
                connections_created += 1
                active_connections.add(id(self))
            return original_init(self, *args, **kwargs)

        def close_wrapper(self, *args, **kwargs):
            with lock:
                active_connections.discard(id(self))
            return original_close(self, *args, **kwargs)

        if schema == "http":
            patched_init = patch.object(
                urllib3.connection.HTTPConnection, '__init__', new=init_wrapper)
            patched_close = patch.object(
                urllib3.connection.HTTPConnection, 'close', new=close_wrapper)
        else:
            patched_init = patch.object(
                urllib3.connection.HTTPSConnection, '__init__', new=init_wrapper)
            patched_close = patch.object(
                urllib3.connection.HTTPSConnection, 'close', new=close_wrapper)

        # Configure with pool size of 5
        pool_size = 5

        with patched_init, patched_close:
            lb = AlternatorLB(Config(
                schema=schema,
                nodes=self.initial_nodes,
                port=self.http_port if schema == "http" else self.https_port,
                datacenter="fake_dc",
                update_interval=0,
                max_pool_connections=pool_size,
            ))

            dynamodb = lb.new_boto3_dynamodb_client()

            lb._update_live_nodes()

            print(f"Known nodes: {lb.get_known_nodes()}")

            num_requests = 50
            num_threads = 10

            def make_request(_):
                try:
                    dynamodb.delete_table(TableName="FakeTable")
                except Exception as e:
                    if e.__class__.__name__ != "ResourceNotFoundException":
                        raise

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                list(executor.map(make_request, range(num_requests)))

            connections_created_first_batch = connections_created

            print(f"Connections created: {connections_created_first_batch}, Active connections: {len(active_connections)}")

            # Active connections should include:
            # - pool_size connections for boto3 client
            # - 1 connection for AlternatorLB's internal node discovery
            expected_max_connections = pool_size + 1
            assert len(active_connections) == expected_max_connections, \
                f"Too many active connections: {len(active_connections)}. Expected <= {expected_max_connections} ({pool_size} for client + 1 for AlternatorLB)."

            for _ in range(20):
                try:
                    dynamodb.delete_table(TableName="FakeTable")
                except Exception as e:
                    if e.__class__.__name__ != "ResourceNotFoundException":
                        raise

            print(f"After second batch - Connections created: {connections_created}, Active connections: {len(active_connections)}")

            # Verify that active connections remain stable after sequential requests
            assert len(active_connections) == expected_max_connections, \
                f"Active connections increased after sequential requests: {len(active_connections)}. Expected <= {expected_max_connections}."

            # Verify that no new connections were created during the second batch
            assert connections_created == connections_created_first_batch, \
                f"New connections were created during second batch: {connections_created}. Expected {connections_created_first_batch}."

    def test_check_if_rack_and_datacenter_set_correctly_wrong_dc(self):
        lb = AlternatorLB(Config(nodes=self.initial_nodes,
                          port=self.http_port, datacenter="datacenter1"))
        lb.check_if_rack_and_datacenter_set_correctly()

    @staticmethod
    def _run_create_add_delete_test(dynamodb):
        TABLE_NAME = "TestTable"
        ITEM_KEY = {'UserID': {'S': '123'}}

        try:
            dynamodb.delete_table(TableName=TABLE_NAME)
        except Exception as e:
            if e.__class__.__name__ != "ResourceNotFoundException":
                raise

        print("Creating table...")
        dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[{'AttributeName': 'UserID',
                        'KeyType': 'HASH'}],  # Primary Key
            AttributeDefinitions=[
                {'AttributeName': 'UserID', 'AttributeType': 'S'}],  # String Key
            ProvisionedThroughput={
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print(f"Table '{TABLE_NAME}' creation started.")

        # 2️⃣ Add an Item
        print("Adding item to the table...")
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'UserID': {'S': '123'},
                'Name': {'S': 'Alice'},
                'Age': {'N': '25'}
            }
        )
        print("Item added.")

        # 3️⃣ Get the Item
        print("Retrieving item...")
        response = dynamodb.get_item(TableName=TABLE_NAME, Key=ITEM_KEY)
        if 'Item' in response:
            print("Retrieved Item:", response['Item'])
        else:
            print("Item not found.")

        # 4️⃣ Delete the Item
        print("Deleting item...")
        dynamodb.delete_item(TableName=TABLE_NAME, Key=ITEM_KEY)

    def test_botocore_create_add_delete(self):
        lb = AlternatorLB(Config(
            nodes=self.initial_nodes,
            port=self.http_port,
            datacenter="datacenter1",
        ))
        self._run_create_add_delete_test(lb.new_botocore_dynamodb_client())

    def test_boto3_create_add_delete(self):
        lb = AlternatorLB(Config(
            nodes=self.initial_nodes,
            port=self.http_port,
            datacenter="datacenter1",
        ))
        self._run_create_add_delete_test(lb.new_boto3_dynamodb_client())
