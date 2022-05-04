from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth
import io
from googleapiclient.http import MediaIoBaseDownload
from pathlib import Path

SCOPES = ["https://www.googleapis.com/auth/drive"]


class CCGPDrive:
    """Class for interacting with CCGP Data Wrangling drive."""

    def __init__(self) -> None:
        """Gets credentials and builds google drive service."""
        self.creds, _ = google.auth.default(scopes=SCOPES)
        self.service = build("drive", "v3", credentials=self.creds)

    def _get_files_list_response(self, query: str) -> list[dict]:
        page_token = None
        result = []
        while True:
            response = (
                self.service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="nextPageToken, files(id, name, modifiedTime)",
                    pageToken=page_token,
                )
                .execute()
            )
            files = response.get("files", [])
            for file in files:
                result.append(file)

            page_token = response.get(
                "nextPageToken", None
            )  # Drive API iterates each page of the drive, so we have to do this to make sure we search each page. Doesn't matter for this case but its best practices
            if page_token is None:
                break
        return result

    def list_files_from_folder(self, folder: str) -> list[dict]:
        def get_folder_id(folder: str) -> str:
            query = (
                f"name = '{folder}' and mimeType = 'application/vnd.google-apps.folder'"
            )
            found = self._get_files_list_response(query)
            if len(found) == 0:
                raise AssertionError(
                    f"Search for folder '{folder}' returned no results."
                )
            result = found[0].get("id")
            return result

        folder_id = get_folder_id(folder)
        query = f"'{folder_id}' in parents"
        found = self._get_files_list_response(query)
        return found

    def download_files(self, *files: dict) -> None:
        for file in files:
            if Path(file["name"]).exists():
                print(
                    "File: "
                    + "'"
                    + file["name"]
                    + "'"
                    + " already exists, skipping download."
                )
                continue
            request = self.service.files().get_media(fileId=file.get("id"))
            fh = io.FileIO(file.get("name"), "wb")
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            print("Downloading file " + "'" + file["name"] + "'")
            while done is False:
                status, done = downloader.next_chunk()
