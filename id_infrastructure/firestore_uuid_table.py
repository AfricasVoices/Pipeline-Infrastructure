import uuid

from core_data_modules.logging import Logger

from util.firestore_utils import make_firestore_client

BATCH_SIZE = 500
_UUID_KEY_NAME = "uuid"

log = Logger(__name__)


class FirestoreUuidInfrastructure(object):
    def __init__(self, client):
        """
        Client for accessing a collection of Firestore uuid tables.

        :param client: Firebase client.
        :type client: firebase_admin.auth.Client
        """
        self._client = client

    @classmethod
    def init_from_credentials(cls, cert, app_name="FirestoreUuidInfrastructure"):
        """
        :param cert: Firestore service account certificate, as a path to a file or a dictionary.
        :type cert: str | dict
        :param app_name: Name to call the Firestore app instance we'll use to connect.
        :type app_name: str
        :return:
        :rtype: FirestoreUuidInfrastructure
        """
        return cls(make_firestore_client(cert, app_name))

    def list_table_names(self):
        """
        :return: The names of all the firestore uuid tables currently in Firestore.
        :rtype: list of str
        """
        tables = self._client.collection("tables").get()
        return [table.id for table in tables]

    def get_table(self, table_name, uuid_prefix):
        """
        :param table_name: Name of table to get.
        :type table_name: str
        :param uuid_prefix: Prefix to give the generated uuids in the table.
        :type uuid_prefix: str
        :return: FirestoreUuidTable with name `table_name`.
        :rtype: FirestoreUuidTable
        """
        return FirestoreUuidTable(self._client, table_name, uuid_prefix)


class FirestoreUuidTable(object):
    def __init__(self, client, table_name, uuid_prefix):
        """
        Client for accessing a single Firestore uuid table.

        :param client: Firebase client.
        :type client: firebase_admin.auth.Client
        :param table_name: Name of the uuid table in Firestore.
        :type table_name: str
        :param uuid_prefix: Prefix to give the generated uuids in the table.
        :type uuid_prefix: str
        """
        self._client = client
        self._table_name = table_name
        self._uuid_prefix = uuid_prefix
        self._mappings_cache = dict()  # of data -> uuid

    @classmethod
    def init_from_credentials(cls, cert, table_name, uuid_prefix, app_name="FirestoreUuidInfrastructure"):
        """
        :param cert: Firestore service account certificate, as a path to a file or a dictionary.
        :type cert: str | dict
        :param table_name: Name of the uuid table in Firestore.
        :type table_name: str
        :param uuid_prefix: Prefix to give the generated uuids in the table.
        :type uuid_prefix: str
        :param app_name: Name to call the Firestore app instance we'll use to connect.
        :type app_name: str
        :return:
        :rtype: FirestoreUuidTable
        """
        return cls(make_firestore_client(cert, app_name), table_name, uuid_prefix)

    def _mappings_ref(self):
        return self._client.collection(f"tables/{self._table_name}/mappings")

    def data_to_uuid_batch(self, list_of_data_requested):
        # Serve the request from the cache if possible, saving network request time + Firestore read costs
        list_of_data_requested = set(list_of_data_requested)
        if len(list_of_data_requested - set(self._mappings_cache.keys())) == 0:
            log.info(f"Returning uuids for {len(list_of_data_requested)} data items from cache...")
            return {data: uuid for data, uuid in self._mappings_cache.items() if data in list_of_data_requested}

        # If the cache is empty, download the entire mappings dataset for this table.
        # Otherwise, assume the cache is up-to-date and use that (we'll still check before creating new uuids and
        # overwriting any existing data just in case it's not up-to-date, which likely means the table was being used
        # concurrently)
        if len(self._mappings_cache) == 0:
            log.info(f"Sourcing uuids for {len(list_of_data_requested)} data items from Firestore...")
            existing_mappings = dict()
            for mapping in self._mappings_ref().get():
                existing_mappings[mapping.id] = mapping.get(_UUID_KEY_NAME)
        else:
            log.info(f"Sourcing uuids for {len(list_of_data_requested)} data items from cache...")
            existing_mappings = self._mappings_cache.copy()

        set_of_data_requested = set(list_of_data_requested)
        new_mappings_needed = set_of_data_requested.difference(
            set(existing_mappings.keys()))

        log.info(f"Loaded {len(existing_mappings)} existing mappings. New mappings needed: {len(new_mappings_needed)}")

        new_mappings = dict()
        for data in new_mappings_needed:
            new_mappings[data] = FirestoreUuidTable.generate_new_uuid(self._uuid_prefix)

        # Make sure the table doc exists
        self._client.document(f"tables/{self._table_name}").set({"table_name": self._table_name}, merge=True)

        # Batch write the new mappings
        total_count_to_write = len(new_mappings)
        i = 0
        batch_counter = 0
        batch = self._client.batch()
        for data in new_mappings.keys():
            # ensure in single read that the data doesn't exist
            uuid_doc = self._client.document(f"tables/{self._table_name}/mappings/{data}").get()
            exists = uuid_doc.exists
            if exists:
                log.warning("Attempted to set mapping for data which was already in the datastore. "
                            "Continuing without overwriting")
                existing_mappings[uuid_doc.id] = uuid_doc.get(_UUID_KEY_NAME)
                continue
            
            i += 1
            batch.set(
                self._client.document(f"tables/{self._table_name}/mappings/{data}"),
                {
                    _UUID_KEY_NAME: new_mappings[data]
                })
            batch_counter += 1
            if batch_counter >= BATCH_SIZE:
                batch.commit()
                log.info(f"Batch of {batch_counter} mappings committed, progress: {i} / {total_count_to_write}")
                batch_counter = 0
                batch = self._client.batch()
        
        if batch_counter > 0:
            batch.commit()
            log.info(f"Final batch of {batch_counter} mappings committed")
        
        existing_mappings.update(new_mappings)
        self._mappings_cache.update(existing_mappings)
        
        ret = dict()
        for data_requested in set_of_data_requested:
            ret[data_requested] = existing_mappings[data_requested]
        
        return ret

    def has_data(self, data):
        return self._client.document(f"tables/{self._table_name}/mappings/{data}").get().exists

    def data_to_uuid(self, data):
        # Check if data mapping exists
        # If it does return the UUID
        # If it doesn't, create a new UUID, store it
        # block until the store completes return the new UUID
        if data in self._mappings_cache:
            return self._mappings_cache[data]

        uuid_doc_ref = self._client.document(f"tables/{self._table_name}/mappings/{data}").get()

        exists = uuid_doc_ref.exists

        if not exists:
            new_uuid = FirestoreUuidTable.generate_new_uuid(self._uuid_prefix)
            log.info(f"Creating new UUID {new_uuid}")

            # Make sure the table doc exists
            self._client.document(f"tables/{self._table_name}").set({"table_name": self._table_name}, merge=True)

            # Write the new data <-> uuid mapping
            self._client.document(f"tables/{self._table_name}/mappings/{data}").set(
                {
                    _UUID_KEY_NAME: new_uuid
                }
            )
        else:
            new_uuid = uuid_doc_ref.get(_UUID_KEY_NAME)

        self._mappings_cache[data] = new_uuid

        return new_uuid
    
    def uuid_to_data(self, uuid_to_lookup):
        # Search for the UUID
        # return the data or fail
        uuid_col_ref = self._mappings_ref()

        # Create a query against the collection
        query_ref = uuid_col_ref.where(_UUID_KEY_NAME, u"==", uuid_to_lookup)

        # Execute the query, and return the first uuid found
        # The API doesn't have a get first method, so this
        # unusual iterator extractor is needed 
        for result in query_ref.get():
            return result.id
        raise LookupError() 

    def uuid_to_data_batch(self, uuids_to_lookup):
        # Serve the request from the cache if possible, saving network request time + Firestore read costs
        uuids_to_lookup = set(uuids_to_lookup)
        if len(uuids_to_lookup - set(self._mappings_cache.values())) == 0:
            log.info(f"Looking up the data for {len(uuids_to_lookup)} uuids from cache...")
            return {uuid: data for data, uuid in self._mappings_cache.items() if uuid in uuids_to_lookup}

        # Search for the UUID
        # Return a mapping data for the uuids that were in the collection
        log.info(f"Looking up the data for {len(uuids_to_lookup)} uuids from Firestore...")
        reverse_mappings = dict()
        for mapping in self._mappings_ref().get():
            self._mappings_cache[mapping.id] = mapping.get(_UUID_KEY_NAME)
            reverse_mappings[mapping.get(_UUID_KEY_NAME)] = mapping.id
        
        log.info(f"Loaded {len(reverse_mappings)} mappings")

        results = {}
        for uuid_lookup in uuids_to_lookup:
            if uuid_lookup in reverse_mappings.keys():
                results[uuid_lookup] = reverse_mappings[uuid_lookup]

        log.info(f"Found keys for {len(results)} out of {len(uuids_to_lookup)} requests")
        return results

    def get_all_mappings(self):
        """
        Returns all the mappings currently in this table.

        :return: Dictionary of data -> uuid
        :rtype: dict
        """
        self._mappings_cache = {}
        for mapping in self._mappings_ref().get():
            self._mappings_cache[mapping.id] = mapping.get(_UUID_KEY_NAME)
        return self._mappings_cache.copy()

    @staticmethod
    def generate_new_uuid(prefix):
        return prefix + str(uuid.uuid4())

    def delete_mappings(self, mappings):
        """
        Deletes the mappings specified.

        :param mappings: Dictionary of data -> uuid
        :type mappings: dict of str -> str
        """
        skipped_mappings = 0
        for mapping_id in mappings.keys():
            log.warning(f"Deleting mapping id `{mapping_id}`...")
            mapping_ref = self._mappings_ref().document(mapping_id)
            
            firestore_mapping = mapping_ref.get()
            if not firestore_mapping.exists:
                skipped_mappings += 1
                log.warning(f"Mapping id `{mapping_id}` does not exist. skipping...")

            assert mappings[mapping_id] == firestore_mapping.get(_UUID_KEY_NAME)
            mapping_ref.delete()
        
        log.info(f"Deleted {len(mappings)} mapping(s) and skipped {skipped_mappings} mapping(s) that didn't exist")
