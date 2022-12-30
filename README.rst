Google Drive API
================

Simple :code:`GoogleDriveApplication` class to perform common Google Drive actions
such as searching files, downloading entire folders, uploading files, deleting files.

Usage
-----

.. code-block:: python

    from application import GoogleDriveApplication

    GD = GoogleDriveApplication("path-to-client-secret.json")

    # Find files
    GD.find_files("hello+world", is_folder=False)

    # List entire subtree of the specified folder
    GD.list_folder("google-drive-id-of-folder", recursive=True)

    # Download entire folder
    GD.download_folder("google-drive-id-of-folder")



Create a project on Google Cloud Platform
-----------------------------------------

Wizard: https://console.developers.google.com/start/api?id=drive

**Instructions**:

* https://cloud.google.com/genomics/downloading-credentials-for-api-access
* Select application type as "Installed Application"
* Create credentials OAuth consent screen --> OAuth client ID
* Save :code:`client_secret.json`

References
----------

* https://developers.google.com/api-client-library/python/start/get_started
* https://developers.google.com/drive/v3/reference/
* https://developers.google.com/drive/v3/web/quickstart/python