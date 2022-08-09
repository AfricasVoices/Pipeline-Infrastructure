import os
import time
import socket

import google.oauth2.service_account
import googleapiclient.discovery
from core_data_modules.logging import Logger
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_FOLDER_TYPE = "application/vnd.google-apps.folder"

_drive_service = None

log = Logger(__name__)


def init_client_from_file(service_account_credentials_file):
    global _drive_service

    credentials = google.oauth2.service_account.Credentials.from_service_account_file(service_account_credentials_file,
                                                                                      scopes=SCOPES)
    if not credentials:
        log.error(f"Failed to get credentials from file '{service_account_credentials_file}'")
        exit(1)

    _drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)


def init_client_from_info(service_account_credentials_info):
    global _drive_service

    credentials = google.oauth2.service_account.Credentials.from_service_account_info(service_account_credentials_info,
                                                                                      scopes=SCOPES)
    if not credentials:
        log.error("Failed to get credentials from dict")
        exit(1)

    _drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)


def _get_root_id():
    log.info("Getting id of drive root folder...")
    return _drive_service.files().get(fileId='root').execute().get('id')


def _list_folder_id(folder_id):
    """Returns a list with map elements with the following structure:
    {
        'name': ''
        'id': '',
        'mimeType': '',
    }
    """
    children = []
    page_token = None

    log.info(f"Getting children of folder with id '{folder_id}'...")
    page_count = 1
    while True:
        response = _drive_service.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token).execute()
        log.info(f"Getting children of folder with id '{folder_id}' - got page {page_count}")
        for file in response.get("files", []):
            children.append(file)
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
        page_count += 1
    log.info(f"Getting children of folder with id '{folder_id}' - done. {len(children)} children")
    return children


def _get_folder_id(name, parent_id, recursive=False):
    log.info(f"Getting id of folder '{name}' under parent with id '{parent_id}'...")
    response = _drive_service.files().list(
        q=f"name='{name}' and '{parent_id}' in parents and mimeType='{DRIVE_FOLDER_TYPE}'",
        spaces='drive',
        fields='files(id)').execute()
    files = response.get('files', [])
    if len(files) == 0:
        if not recursive:
            log.error(f"Folder '{name}' not found under parent with id {parent_id}.")
            exit(1)
        # Create folder
        files.append({"id": _add_folder(name, parent_id)})
    if len(files) > 1:
        log.error(f"Multiple folders with name '{name}' found under parent with id {parent_id}.")
        exit(1)
    assert (len(files) == 1)
    folder = files[0]
    folder_id = folder.get("id")
    log.info(
        f"Getting id of folder '{name}' under parent with id '{parent_id}' - done. Folder id is '{folder_id}'")
    return folder_id


def _get_shared_folder_id(name):
    log.info(f"Getting id of shared-with-me folder '{name}'...")
    response = _drive_service.files().list(
        q=f"name='{name}' and sharedWithMe=true and mimeType='{DRIVE_FOLDER_TYPE}'",
        spaces="drive",
        fields="files(id)").execute()
    files = response.get("files", [])
    if len(files) == 0:
        log.error(f"Folder '{name}' not found in shared-with-me category.")
        exit(1)
    if len(files) > 1:
        log.error(f"Multiple folders with name '{name}' found in shared-with-me category.")
        exit(1)
    assert (len(files) == 1)
    folder = files[0]
    folder_id = folder.get('id')
    log.info(f"Getting id of shared-with-me folder '{name}' - done. Folder id is '{folder_id}'")
    return folder_id


def _get_path_id(path, recursive=False, target_folder_is_shared_with_me=False):
    folders = _split_path(path)

    if target_folder_is_shared_with_me:
        if len(folders) == 0:
            log.error("Missing target folder name which necessary when looking for a shared-with-me type folder")
            exit(1)
        folder_id = _get_shared_folder_id(folders[0])
        folders.remove(folders[0])
    else:
        folder_id = _get_root_id()

    for folder in folders:
        folder_id = _get_folder_id(folder, folder_id, recursive)
    return folder_id


def _split_path(path):
    folders = []
    while path != "":
        path, folder = os.path.split(path)
        if folder != "":
            folders.append(folder)
    folders.reverse()
    return folders


def _add_folder(name, parent_id):
    log.info(f"Creating folder '{name}' under parent with id '{parent_id}'...")
    file_metadata = {
        "name": name,
        "mimeType": DRIVE_FOLDER_TYPE,
        "parents": [parent_id],
    }
    file = _drive_service.files().create(body=file_metadata,
                                         fields="id").execute()
    log.info(f"Creating folder '{name}' under parent with id '{parent_id}' - done. Folder id is '{file.get('id')}'")
    return file.get('id')


def _update_file(source_file_path, target_file_id):
    media = MediaFileUpload(source_file_path,
                            resumable=True)

    log.info(f"Updating file with ID '{target_file_id}' with source file '{source_file_path}'...")
    file = _drive_service.files().update(fileId=target_file_id,
                                         media_body=media,
                                         fields="name").execute()

    log.info(
        f"Updating file with ID '{target_file_id}' with source file '{source_file_path}' - done. File name was "
        f"'{file.get('name')}'"
    )


def _create_file(source_file_path, target_folder_id, target_file_name=None):
    if target_file_name is None:
        target_file_name = os.path.basename(source_file_path)

    file_metadata = {
        "name": target_file_name,
        "parents": [target_folder_id]
    }
    media = MediaFileUpload(source_file_path,
                            resumable=True)

    log.info(f"Creating file '{target_file_name}' in folder with ID '{target_folder_id}' "
             f"with source file '{source_file_path}'...")
    file = _drive_service.files().create(body=file_metadata,
                                         media_body=media,
                                         fields="id").execute()
    log.info(f"Creating file '{target_file_name}' in folder with ID '{target_folder_id}' with source file "
             f"'{source_file_path}' - done. File id is '{file.get('id')}'")


def _delete_file(file_id):
    """
    Permanently deletes a file, skipping the trash.
    """
    log.warning(f"Deleting file '{file_id}'...")
    _drive_service.files().delete(fileId=file_id).execute()
    log.info(f"Deleting file '{file_id}' - done.")


def _auto_retry(f, max_retries=2, backoff_seconds=1):
    try:
        return f()
    except (HttpError, socket.timeout) as ex:
        if type(ex) == HttpError:
            if ex.resp.status not in {500, 503}:
                raise ex
            log.warning(f"Drive call failed with HttpError {ex.resp.status}")

        if type(ex) == socket.timeout:
            log.warning(f"Drive call failed with socket.timeout error")

        if max_retries > 0:
            log.info(f"Retrying up to {max_retries} more times, after {backoff_seconds} seconds...")
            time.sleep(backoff_seconds)
            _auto_retry(f, max_retries - 1, backoff_seconds * 2)
        else:
            log.error("Retried the maximum number of times")
            raise ex


def update_or_create_batch(source_file_paths, target_folder_path, recursive=False,
                           target_folder_is_shared_with_me=False, fix_duplicates=False,
                           max_retries=2, backoff_seconds=1):
    target_folder_id = _auto_retry(lambda: _get_path_id(target_folder_path, recursive, target_folder_is_shared_with_me),
                                   max_retries, backoff_seconds)
    files = _auto_retry(lambda: _list_folder_id(target_folder_id), max_retries, backoff_seconds)
    
    for i, source_file_path in enumerate(source_file_paths):
        log.info(f"Uploading file {i + 1}/{len(source_file_paths)}: {source_file_path}...")
        
        target_file_name = os.path.basename(source_file_path)
        files_with_upload_name = list(filter(lambda file: file.get('name') == target_file_name, files))

        if len(files_with_upload_name) > 1:
            log.warning(f"Multiple files with the same name '{source_file_path}' found in Drive folder.")
            if fix_duplicates:
                log.warning("Deleting the duplicate files...")
                for duplicate_file in files_with_upload_name:
                    # Make sure it's not a folder
                    if duplicate_file.get("mimetype") == DRIVE_FOLDER_TYPE:
                        log.error(f"Attempting to remove a folder with name '{target_file_name}'")
                        exit(1)
                    _auto_retry(lambda: _delete_file(duplicate_file.get("id")), max_retries, backoff_seconds)
                files_with_upload_name = []
            else:
                log.error("I don't know which to update, aborting. To handle this automatically in future, set "
                          "`fix_duplicates=True` when calling this function.")
                exit(1)

        if len(files_with_upload_name) == 1:
            existing_file = files_with_upload_name[0]
            # Make sure it's not a folder
            if existing_file.get("mimetype") == DRIVE_FOLDER_TYPE:
                log.error(f"Attempting to replace a folder with a file with name '{target_file_name}'")
                exit(1)
            _auto_retry(lambda: _update_file(source_file_path, existing_file.get("id")), max_retries, backoff_seconds)
            continue

        _auto_retry(lambda: _create_file(source_file_path, target_folder_id, target_file_name),
                    max_retries, backoff_seconds)


def update_or_create(source_file_path, target_folder_path, target_file_name=None, recursive=False,
                     target_folder_is_shared_with_me=False, fix_duplicates=False,
                     max_retries=2, backoff_seconds=1):
    if target_file_name is None:
        target_file_name = os.path.basename(source_file_path)

    target_folder_id = _auto_retry(
        lambda: _get_path_id(target_folder_path, recursive, target_folder_is_shared_with_me),
        max_retries, backoff_seconds)
    files = _auto_retry(lambda: _list_folder_id(target_folder_id), max_retries, backoff_seconds)

    files_with_upload_name = list(filter(lambda file: file.get('name') == target_file_name, files))

    if len(files_with_upload_name) > 1:
        log.warning(f"Multiple files with the same name '{target_file_name}' found in Drive folder.")
        if fix_duplicates:
            log.warning("Deleting the duplicate files...")
            for duplicate_file in files_with_upload_name:
                # Make sure it's not a folder
                if duplicate_file.get("mimetype") == DRIVE_FOLDER_TYPE:
                    log.error(f"Attempting to remove a folder with name '{target_file_name}'")
                    exit(1)
                _auto_retry(lambda: _delete_file(duplicate_file.get("id")), max_retries, backoff_seconds)
            files_with_upload_name = []
        else:
            log.error("I don't know which to update, aborting. To handle this automatically in future, set "
                      "`fix_duplicates=True` when calling this function.")
            exit(1)

    if len(files_with_upload_name) == 1:
        existing_file = files_with_upload_name[0]
        # Make sure it's not a folder
        if existing_file.get("mimetype") == DRIVE_FOLDER_TYPE:
            log.error(f"Attempting to replace a folder with a file with name '{target_file_name}'")
            exit(1)
        _auto_retry(lambda: _update_file(source_file_path, existing_file.get("id")), max_retries, backoff_seconds)
        return

    _auto_retry(lambda: _create_file(source_file_path, target_folder_id, target_file_name),
                max_retries, backoff_seconds)


def get_storage_quota():
    """
    Gets the storage quota information for this account.

    :return: Dictionary with keys "limit", "usage", "usageInDrive", and "usageInDriveTrash".
    :rtype: dict of str -> str
    """
    return _drive_service.about().get(fields="storageQuota").execute()["storageQuota"]


def list_all_objects_in_drive(object_properties=None):
    """
    :param object_properties: Object properties to include in the returned data. If None, defaults to "name", "id",
                              "ownedByMe", "mimeType", and "quotaBytesUsed".
    :type object_properties: list of str | None
    :return: List of all objects in this account's drive, annotated with the requested properties.
    :rtype: list of (dict of str -> str)
    """
    if object_properties is None:
        object_properties = ["name", "id", "ownedByMe", "mimeType", "quotaBytesUsed"]
    fields = f"nextPageToken, files({','.join(object_properties)})"

    all_objects = []
    page_results = _drive_service.files().list(spaces="drive", fields=fields).execute()
    all_objects.extend(page_results.get("files", []))
    log.info(f"Fetched 1 page, {len(all_objects)} total objects")
    pages = 1
    while "nextPageToken" in page_results:
        page_results = _drive_service.files().list(
            spaces="drive", fields=fields, pageToken=page_results["nextPageToken"]).execute()
        all_objects.extend(page_results.get("files", []))
        pages += 1
        log.info(f"Fetched {pages} pages, {len(all_objects)} total objects")

    return all_objects


def delete_object(object_id):
    """
    Deletes the object with the given id from Drive. The file is deleted immediately, without going to Trash.

    :param object_id: Drive object id of the object to delete.
    :type object_id: str
    """
    log.warning(f"Deleting Google Drive object with id '{object_id}'...")
    _drive_service.files().delete(fileId=object_id).execute()

def transfer_object_ownership(object_id, new_owner_email_address):
    """
    Transfers the object ownership to a different user with the given email address.

    :param object_id: Drive object id of the object to delete.
    :type object_id: str
    :param new_owner_email_address
    :type new_owner_email_address: str
    """
    log.warning(f"Transferring ownership of Google Drive object with id '{object_id}' to {new_owner_email_address}...")

    new_permission = {
                    'emailAddress' : new_owner_email_address,
                    'type' : 'user',
                    'role' : 'owner'
                }
    '''
    file_permissions = _drive_service.permissions().list(fileId=object_id).execute()
    permission_id = None
    for permission_dict in file_permissions['permissions']:
        if permission_dict['role'] == 'owner':
            permission_id = permission_dict['id']
    '''
    _drive_service.permissions().create(fileId=object_id, body=new_permission).execute()