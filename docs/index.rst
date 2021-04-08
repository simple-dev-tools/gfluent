Welcome to gfluent's documentation!
===================================

This is a wrapper on Google Cloud Platform Python SDK client library. 
It provides a fluent-style to call the methods, here is an example,

.. code-block:: python

   from gfluent import BQ

   project_id = "here-is-you-project-id"
   bq = BQ(project_id, table="mydataset.table")

   count = (
      bq.mode("WRITE_APPEND")
        .sql("SELECT name, age from dataset.tabble")
        .query()
      )

   print(f"{count} rows loaded")


API Reference
=============

.. autoclass:: gfluent.bq.BQ
    :members:

.. autoclass:: gfluent.gcs.GCS
    :members:


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
