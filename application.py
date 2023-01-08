#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Drive Application
========================

Create a project on Google Cloud Platform
-----------------------------------------

Wizard: https://console.developers.google.com/start/api?id=drive

**Instructions**:

* https://cloud.google.com/genomics/downloading-credentials-for-api-access
* Select application type as "Installed Application"
* Create credentials OAuth consent screen --> OAuth client ID
* Save client_secret.json

References
----------

* https://developers.google.com/api-client-library/python/start/get_started
* https://developers.google.com/drive/v3/reference/
* https://developers.google.com/drive/v3/web/quickstart/python
"""

###############################################################################


import io
import os
import json

# import time
import enum
import logging

# import mimetypes
# import multiprocessing as mp
from typing import List, Dict
from dataclasses import dataclass, field

from tqdm import tqdm

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

###############################################################################

from errors import retry

###############################################################################

LOGGER = logging.getLogger(__name__)

###############################################################################

SCOPES = ["https://www.googleapis.com/auth/drive"]

###############################################################################

TYPE_GOOGLE_FILE = "application/vnd.google-apps.file"
TYPE_GOOGLE_FOLDER = "application/vnd.google-apps.folder"
TYPE_GOOGLE_SHORTCUT = "application/vnd.google-apps.shortcut"

TYPE_GOOGLE_SPREADSHEET = "application/vnd.google-apps.spreadsheet"
TYPE_GOOGLE_DOCUMENT = "application/vnd.google-apps.document"
TYPE_GOOGLE_PRESENTATION = "application/vnd.google-apps.presentation"

TYPE_GOOGLE_PHOTO = "application/vnd.google-apps.photo"
TYPE_GOOGLE_AUDIO = "application/vnd.google-apps.audio"
TYPE_GOOGLE_VIDEO = "application/vnd.google-apps.video"

TYPE_GOOGLE_FORM = "application/vnd.google-apps.form"
TYPE_GOOGLE_SITE = "application/vnd.google-apps.site"

TYPE_GOOGLE_SCRIPT = "application/vnd.google-apps.script"

###############################################################################


class Status(enum.Enum):
    SUCCESS = "Done!"
    ALREADY = "Already done!"
    ERROR = "Something went wrong!"


###############################################################################


@dataclass
class GoogleDriveApplication:
    """
    Google Drive Application
    """

    client_secret: str
    credentials_path: str = field(default=None, repr=False)
    scopes: str = field(default=None)

    def __post_init__(self):
        if self.scopes is None:
            self.scopes = SCOPES

        if self.credentials_path is None:
            self.credentials_path = os.path.join(
                os.path.expanduser("~"), ".credentials", "drive_token.json"
            )

        creds = self.get_credentials()
        self.drive_service = build("drive", "v3", credentials=creds)

    def get_credentials(self) -> Credentials:
        """Get valid user credentials

        If no (valid) credentials are available,
        * Log the user in
        * Store the credentials for future use

        Returns
        -------
        Credentials or None
            Valid user credentials
        """
        if os.path.isfile(self.credentials_path):
            creds = Credentials.from_authorized_user_file(
                self.credentials_path, self.scopes
            )
        else:
            credential_dir = os.path.dirname(self.credentials_path)
            os.makedirs(credential_dir, exist_ok=True)
            creds = None

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secrets_file=self.client_secret, scopes=self.scopes
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            LOGGER.info(f"Storing credentials to {self.credentials_path}")
            with open(self.credentials_path, "w") as token:
                token.write(creds.to_json())

        return creds

    # ----------------------------------------------------------------------- #
    # Drive Actions

    @retry()
    def search_files(self, query_string: str) -> List[Dict[str, str]]:
        """Search file in drive location

        Parameters
        ----------
        query_string : str
            Valid query string (value of parameter :code:`q`

        Returns
        -------
        List[Dict[str, str]]
            List of file with metadata

        Reference
        ---------
        https://developers.google.com/drive/api/guides/search-files#examples
        """
        files = []
        page_token = None
        file_fields = [
            "id",
            "name",
            "kind",
            "mimeType",
            "size",
            "createdTime",
            "modifiedTime",
        ]
        while True:
            # pylint: disable=maybe-no-member
            response = (
                self.drive_service.files()
                .list(
                    q=query_string,
                    spaces="drive",
                    fields=f"nextPageToken, files({','.join(file_fields)})",
                    pageToken=page_token,
                )
                .execute()
            )

            for file in response.get("files", []):
                LOGGER.info(f'Found: {file.get("name")}, {file.get("id")}')
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

        return files

    @retry()
    def find_files(
        self, search_string: str, is_folder: bool = None, parent_id: str = None
    ) -> List[Dict[str, str]]:
        """Find files with useful search strings

        Parameters
        ----------
        search_string : str
            Strings to search in name
            '+' can be used to provide multiple search strings
            '!' can be appended to strings that should not appear in the name
            e.g.,
            "hello+!cruel+world": search for files whose name,
              - contains strings "hello" and "world"
              - does not contain "cruel"
        is_folder : bool, optional
            If True, search only folders
            If False, search only files
            If None, do not impose any condition for folder type
            The default is None
        parent_id : str, optional
            If not None, search only among files with the specified parent
            The default is None

        Returns
        -------
        List[Dict[str, str]]
            List of file with metadata
        """
        string_parts = search_string.split("+")
        conditions = []
        for string_part in string_parts:
            if string_part.startswith("!"):
                conditions.append(f"not name contains '{string_part[1:]}'")
            else:
                conditions.append(f"name contains '{string_part}'")

        if is_folder is not None:
            comparison_sign = "=" if is_folder else "!="
            conditions.append(
                f"mimeType {comparison_sign} '{TYPE_GOOGLE_FOLDER}'"
            )

        if parent_id is not None:
            conditions.append(f"'{parent_id}' in parents")

        query_string = " and ".join(conditions)
        return self.search_files(query_string)

    # ----------------------------------------------------------------------- #

    @retry()
    def create_folder(self, name):
        file_metadata = {"name": name, "mimeType": TYPE_GOOGLE_FOLDER}
        folder = (
            self.drive_service.files()
            .create(body=file_metadata, fields="id, name, parents, mimeType")
            .execute()
        )

        return folder.get("id")

    def list_folder(
        self, folder_id: str, recursive: bool = False, prefix: str = "."
    ) -> List[Dict[str, str]]:
        """List contents of a folder

        Parameters
        ----------
        folder_id : str
            ID of the folder on Google Drive
        recursive : bool
            If True, list contents of folders recursively
            The default is False
        prefix : str
            Prefix to apply to files if any
            The default is '.'

        Returns
        -------
        List[Dict[str, str]]
            List of file with metadata
        """
        query_string = f"'{folder_id}' in parents"
        result = []

        for file in self.search_files(query_string):
            file_id = file.get("id")
            file_name = file.get("name")
            file["path"] = f"{prefix}/{file_name}"
            LOGGER.info(file["path"])
            result.append(file)

            is_folder = file.get("mimeType") == TYPE_GOOGLE_FOLDER
            if is_folder and recursive:
                subfiles = self.list_folder(
                    file_id,
                    recursive=recursive,
                    prefix=f"{prefix}/{file_name}",
                )
                result.extend(subfiles)

        return result

    def download_folder(
        self, folder_id: str, output_path: str = ".", resume: bool = True
    ):
        """Download entire folder

        Parameters
        ----------
        folder_id : str
            Google Drive ID of the folder to be downloaded
        output_path : str, optional
            Local path of the desired download location
            The default is "."
        resume : bool, optional
            If True, resume download
            (i.e. do not download if a file exists at the file['path'])
        """
        files = self.list_folder(folder_id, recursive=True, prefix=output_path)
        skipped = []
        with open(
            os.path.join(output_path, f"{folder_id}.filelist.json"),
            mode="w",
            encoding="utf-8",
        ) as f:
            json.dump(files, f, ident=2, ensure_ascii=False)

        for file in tqdm(files):
            is_folder = file.get("mimeType") == TYPE_GOOGLE_FOLDER
            if not is_folder:
                if os.path.isfile(file.get("path")):
                    continue
                try:
                    self.download_file(file.get("id"), file.get("path"))
                except Exception:
                    LOGGER.error(f"Couldn't download {file.get('id')}")
                    skipped.append(file)

        with open(
            os.path.join(output_path, f"{folder_id}.skipped.json"),
            mode="w",
            encoding="utf-8",
        ) as f:
            json.dump(skipped, f, indent=2, ensure_ascii=False)

    @retry()
    def upload_file(
        self, local_path: str, upload_parent_id: str = None
    ) -> str:
        """Upload file

        Parameters
        ----------
        local_path : str
            Path of the file to be uploaded
        upload_parent_id : str, optional
            Google Drive ID of the parent folder
            If None, root folder will be used
            The default is None

        Returns
        -------
        str
            Google Drive ID of the file uploaded
        """
        upload_parent_id = upload_parent_id or "root"
        file_metadata = {"parents": [upload_parent_id]}
        media = MediaFileUpload(local_path, resumable=True)
        file = (
            self.drive_service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, parents, mimeType",
            )
            .execute()
        )
        file_id = file.get("id")
        file_name = file.get("name")
        LOGGER.info(
            f"File '{local_path}' uploaded: '{file_name}' (id: '{file_id}')"
        )
        return file_id

    @retry()
    def download_file(self, file_id: str, output_path: str = None):
        """Download a file

        Parameters
        ----------
        file_id : str
            ID of the file on Google Drive to be downloaded
        output_path : str
            Path to store output
        """
        request = self.drive_service.files().get_media(fileId=file_id)
        if output_path is not None:
            output_path_parent = os.path.dirname(output_path)
            os.makedirs(output_path_parent, exist_ok=True)
            file = io.FileIO(output_path, "wb")
        else:
            file = io.BytesIO()

        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            LOGGER.info(f"Download {int(status.progress() * 100)}.")

        if output_path is None:
            return file.getvalue()

    @retry()
    def delete_file(self, file_id: str):
        """Delete a file from Google Drive

        Parameters
        ----------
        file_id : str
            ID of the file on Google Drive to be deleted
        """
        self.drive_service.files().delete(fileId=file_id).execute()
        LOGGER.info(f"File '{file_id}' deleted from Google Drive.")

    @staticmethod
    def dump_files_info(files: List[Dict[str, str]], prefix: str = "info."):
        with open(f"{prefix}.file.json", "w", encoding="utf-8") as f:
            json.dump(files, f, indent=2, ensure_ascii=False)
        with open(f"{prefix}.paths.txt", "w", encoding="utf-8") as f:
            f.write("\n".join([file.get("path") for file in files]))


###############################################################################
