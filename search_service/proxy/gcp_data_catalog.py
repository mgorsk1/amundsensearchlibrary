# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import re

from google.api_core.client_options import ClientOptions
from google.cloud import datacatalog_v1
from typing import Any, List, Dict, Union, Optional

from search_service.models.dashboard import SearchDashboardResult, Dashboard
from search_service.models.table import SearchTableResult
from search_service.models.table import Table
from search_service.models.user import SearchUserResult
from search_service.proxy import BaseProxy

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)


# @todo - consider moving base class with init to commons - it's exactly the same in metadatalibrary
class GCPDataCatalogProxy(BaseProxy):
    def __init__(self, *,
                 project_id: str = '',
                 credentials_file: Optional[str] = None,
                 client: datacatalog_v1.DataCatalogClient = None,
                 page_size: int = 10,
                 **kwargs
                 ) -> None:

        # Scope object will limit all our Data Catalog searches to configured project_id
        self.scope = datacatalog_v1.types.SearchCatalogRequest.Scope()
        self.scope.include_project_ids.append(project_id)

        client_options = ClientOptions(scopes=[self.scope])

        if credentials_file:
            _client = datacatalog_v1.DataCatalogClient.from_service_account_file(credentials_file,
                                                                                 client_options=client_options)
        else:
            _client = datacatalog_v1.DataCatalogClient(credentials=credentials_file, client_options=client_options)

        self.client = client or _client
        self.page_size = page_size

    # @todo make part of commons class
    def _get_resource_metadata(self, resource_link, display_name_regex: str = r'.*\- Metadata$') -> List[dict]:
        result = {}

        display_name_pattern = re.compile(display_name_regex)

        tags = self.client.list_tags(resource_link)

        _entries = {}
        for tag in tags:
            if display_name_pattern.match(tag.template_display_name):
                for k, spec in dict(tag.fields).items():
                    result[k] = spec.string_value

        return result

    def _process_resource(self, entry, entry_type):
        if entry_type == 'table':
            processor = self._process_table_resource
        elif entry_type == 'workbook':
            processor = self._process_dashboard_resource
        else:
            raise NotImplementedError(f'Entry of {entry_type} type not implemented !')

        return processor(entry)

    # @todo make part of commons class
    def _process_table_resource(self, entry: str) -> Table:
        linked_resource_parts = entry.linked_resource.split('/')
        relative_resource_name_parts = entry.relative_resource_name.split('/')

        name = linked_resource_parts[-1]

        _database = entry.user_specified_system or entry.integrated_system

        if isinstance(_database, int):
            if _database == 1:
                database = 'bigquery'
            schema = linked_resource_parts[-3]
        else:
            database = _database
            schema = relative_resource_name_parts[-1].replace(name, '').strip('_')

        cluster = relative_resource_name_parts[1] + '__' + relative_resource_name_parts[3]

        # @todo handle tags and badges/labels
        # for tag in self.client.list_tags(entry.relative_resource_name):
        #     for field in tag.fields:
        #         value = f"{value['display_name']}:{value['string_value']}"
        #
        #         tags.append(value)

        tags = []
        badges = tags

        result = Table(database=database,
                       cluster=cluster,
                       schema=schema,
                       name=name,
                       tags=tags,
                       badges=badges,
                       last_updated_timestamp=None,
                       column_names=[],
                       key=entry.relative_resource_name)

        return result

    def _process_dashboard_resource(self, entry: str) -> Dashboard:
        full_entry = self.client.get_entry(entry.relative_resource_name)

        dashboard_metadata = self._get_resource_metadata(full_entry.name, r'.*Dashboard Metadata$')

        relative_resource_name_parts = full_entry.name.split('/')
        result = Dashboard(cluster=relative_resource_name_parts[1],
                           group_name=dashboard_metadata.get('site_name'),
                           group_url='',
                           url='',
                           product=entry.user_specified_system or entry.integrated_system,
                           name=dashboard_metadata['workbook_name'],
                           last_successful_run_timestamp=0,
                           uri=dashboard_metadata['workbook_entry'])

        return result

    def _basic_search(self, query_term: str, page_index: int, entry_type: str = None, **additional_filters):
        entries = []
        i = 0

        filters = additional_filters
        filters['type'] = entry_type

        query = f'{query_term}'

        # different fields support different separators in search dsl syntax
        # fields like type and system support only exact matching so '=' separator is required
        # for remaining fields we use pattern match with ':' separator
        separators = {'type': '=', 'system': '='}

        for field, values in filters.items():
            separator = separators.get(field, ':')

            if field == 'label':
                if isinstance(values, str):
                    values = values.split(':')
                    if len(values) == 1:
                        label_name = '*'
                        label_value = values[0]
                    elif len(values) == 2:
                        label_name = values[0]
                        label_value = values[1]
                    else:
                        break

                    query = query + f' AND {field}.{label_name}{separator}{label_value}'
            elif isinstance(values, str):
                query = query + f' AND {field}{separator}{values}'
            elif isinstance(values, list):
                _query = ''

                for value in values:
                    _query += f'{field}{separator}{value} OR '

                if _query:
                    query += f' AND ({_query.strip(" OR ")})'

        results = self.client.search_catalog(query=query, scope=self.scope, page_size=self.page_size).pages

        total_count = 0
        page_index_position = 0

        for page in results:
            current_page_count = 0

            total_count += self.page_size

            for element in page:
                current_page_count += 1

                if page_index_position == page_index:
                    result = self._process_resource(element, entry_type)

                    entries.append(result)

            page_index_position += 1

        total_count = total_count - (self.page_size - current_page_count)

        return total_count, entries

    def fetch_table_search_results(self, *, query_term: str, page_index: int = 0, index: str = '') -> SearchTableResult:
        # search for entry of 'table' type will return both tables (type=table) and views (type=table.view)
        total_results, results = self._basic_search(query_term, page_index, 'table')

        return SearchTableResult(total_results=total_results, results=results)

    def fetch_user_search_results(self, *, query_term: str, page_index: int = 0, index: str = '') -> SearchUserResult:
        total_results, results = self._basic_search(query_term, page_index, 'user')

        return SearchTableResult(total_results=total_results, results=results)

    def update_document(self, *, data: List[Dict[str, Any]], index: str = '') -> str:
        pass

    def create_document(self, *, data: List[Dict[str, Any]], index: str = '') -> str:
        pass

    def delete_document(self, *, data: List[str], index: str = '') -> str:
        pass

    def fetch_search_results_with_filter(self, *, query_term: str, search_request: dict, page_index: int = 0,
                                         index: str = '') -> Union[SearchTableResult,
                                                                   SearchDashboardResult]:

        _filters = search_request.get('filters', dict())

        entry_type = index.split('_')[0]

        filter_specs = [
            ('table', 'name', None),
            ('description', 'description', None),
            ('column', 'column', None),
            ('tag', 'tag', None),
            ('database', 'system', None),
            ('badge', 'label', None)
        ]

        filters = {}
        for search_field_name, gcp_field_name, defaults in filter_specs:
            values = _filters.get(search_field_name, defaults)

            if values:
                filters[gcp_field_name] = values

        approx_count, tables = self._basic_search(query_term, page_index, entry_type, **filters)

        return SearchTableResult(total_results=approx_count, results=tables)

    def fetch_dashboard_search_results(self, *, query_term: str, page_index: int = 0,
                                       index: str = '') -> SearchDashboardResult:
        total_results, results = self._basic_search(query_term, page_index, 'workbook')

        return SearchDashboardResult(total_results=total_results, results=results)
