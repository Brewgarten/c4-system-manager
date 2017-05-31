import logging


log = logging.getLogger(__name__)

class TestKeyValueStore():

    def test_delete(self, backend):

        store = backend.keyValueStore

        # add key-value pairs
        store.put("a", "a")
        store.put("b", "b")

        # remove one of the key-value pairs
        store.delete("a")

        # make sure that all key-value pairs have the correct values and there are no duplicates
        assert {("b", "b")} == set(store.getAll())

    def test_deletePrefix(self, backend):

        store = backend.keyValueStore

        # add key-value pairs
        store.put("a", "a")
        store.put("a2", "a2")
        store.put("a3", "a3")
        store.put("b", "b")

        # remove key-value pairs with a specifc prefix
        store.deletePrefix("a")

        # make sure that all key-value pairs have the correct values and there are no duplicates
        assert {("b", "b")} == set(store.getAll())

    def test_get(self, backend):

        store = backend.keyValueStore

        # add key-value pairs
        store.put("a", "a")
        store.put("b", "b")
        store.put("c", "c")

        # make sure that all key-value pairs have the correct values
        assert store.get("a") == "a"
        assert store.get("b") == "b"
        assert store.get("c") == "c"

    def test_getAll(self, backend):

        store = backend.keyValueStore

        # add key-value pairs
        store.put("a", "a")
        store.put("b", "b")
        store.put("c", "c")

        # make sure that all key-value pairs have the correct values and there are no duplicates
        assert {("a", "a"), ("b", "b"), ("c", "c")} == set(store.getAll())

    def test_getPrefix(self, backend):

        store = backend.keyValueStore

        # add key-value pairs
        store.put("a", "a")
        store.put("a2", "a2")
        store.put("a3", "a3")
        store.put("b", "b")

        # make sure that all key-value pairs have the correct values
        assert {("a", "a"), ("a2", "a2"), ("a3", "a3")} == set(store.getPrefix("a"))
        assert {("b", "b")} == set(store.getPrefix("b"))

    def test_put(self, backend):

        store = backend.keyValueStore

        # add key-value pairs
        store.put("a", "a")
        store.put("b", "b")

        # replace a key-value pair
        store.put("a", "newA")

        # make sure that all key-value pairs have the correct values and there are no duplicates
        assert {("a", "newA"), ("b", "b")} == set(store.getAll())

    def test_transaction(self, backend):

        store = backend.keyValueStore

        # add key-value pair
        store.put("a", "a")

        # start transaction
        transaction = store.transaction

        # add key-value pairs
        transaction.put("b", "b")
        transaction.put("c", "c")

        # remove one of the key-value pairs
        transaction.delete("a")

        # make sure that nothing is committed yet
        assert {("a", "a")} == set(store.getAll())

        # commit the transaction
        transaction.commit()
        assert not transaction.statements

        # make sure that all key-value pairs have the correct values
        assert {("b", "b"), ("c", "c")} == set(store.getAll())
