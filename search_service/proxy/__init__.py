# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from threading import Lock

from flask import current_app

from search_service import config
from search_service.proxy.base import BaseProxy
from werkzeug.utils import import_string

_proxy_client = None
_proxy_client_lock = Lock()

DEFAULT_PAGE_SIZE = 10


def get_proxy_client() -> BaseProxy:
    """
    Provides singleton proxy client based on the config
    :return: Proxy instance of any subclass of BaseProxy
    """
    global _proxy_client

    if _proxy_client:
        return _proxy_client

    with _proxy_client_lock:
        if _proxy_client:
            return _proxy_client
        else:
            obj = current_app.config[config.PROXY_CLIENT_KEY]

            # Gather all the configuration to create a Proxy Client
            host = current_app.config[config.PROXY_ENDPOINT]
            user = current_app.config[config.PROXY_USER]
            password = current_app.config[config.PROXY_PASSWORD]

            project_id = current_app.config[config.PROXY_PROJECT_ID]
            credentials_file = current_app.config[config.PROXY_CREDENTIALS_FILE]

            client = import_string(current_app.config[config.PROXY_CLIENT])

            # number of results per search page
            page_size = current_app.config.get(config.SEARCH_PAGE_SIZE_KEY, DEFAULT_PAGE_SIZE)

            _proxy_client = client(host=host, user=user, password=password, client=obj, page_size=page_size,
                                   project_id=project_id, credentials_file=credentials_file)

    return _proxy_client
