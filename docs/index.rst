gfluent's documentation 0.1.13
==============================

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

.. autoclass:: gfluent.BQ
    :members:

.. autoclass:: gfluent.GCS
    :members:

.. autoclass:: gfluent.Sheet
    :members:


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
