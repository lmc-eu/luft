# -*- coding: utf-8 -*-
"""Column."""
from typing import List, Optional, Union

from luft.common.config import EMBULK_TYPE_MAPPER
from luft.common.logger import setup_logger

# Setup logger
logger = setup_logger('common', 'INFO')


class Column:
    """Column."""

    def __init__(self, name: str, data_type: str, rename: Optional[str] = None,
                 escape: Optional[bool] = False, mandatory: Optional[bool] = False,
                 pk: Optional[bool] = False, default_value: Optional[str] = None,
                 ignored: Optional[bool] = False, tech_column: Optional[bool] = False, metadata: Optional[bool] = False):
        """Create column.

        Parameters:
            name (str): column name.
            rename (str): rename column in SQL.
            data_type (str): data type. Varchar, integer, etc.
            escape (bool): wheter to escape column name.
            mandatory (bool): wheter column is mandatory.
            pk (bool): wheter column is primary key.
            default_value (str): default fix value.
            ignored (bool): wheter column is ignored in historization phase.
            tech_column(bool): wheter column is just a technical. Prefixed DW_.
            metadata: wheter column is metadata from elasticsearch

        """
        self.name = name
        self.rename = rename
        self.data_type = data_type
        self.escape = escape
        self.mandatory = mandatory
        self.pk = pk
        self.default_value = default_value
        self.ignored = ignored
        self.tech_column = tech_column
        self.metadata = metadata

        self.index = 0

    def set_index(self, index: int):
        """Set index of column."""
        self.index = index

    def get_name(self, col_type: str = 'all', filter_ignored: bool = True,
                 include_tech: bool = True) -> Optional[str]:
        """Return column name or rename.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (str): column name

        """
        # Decide if value should be returned
        if self._should_return(col_type, filter_ignored, include_tech):
            name = self.rename or self.name
            return name
        return None

    def get_index(self, col_type: str = 'all', filter_ignored: bool = True,
                  include_tech: bool = True) -> Optional[str]:
        """Get indexed value for Loading into Snowflake.

        E.g. `$1 as Column_Name`.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (str): index

        """
        if self._should_return(col_type, filter_ignored, include_tech):
            return f'${self.index} AS {self.get_name()}'
        return None

    def get_aliased_name(self, col_type: str = 'all', filter_ignored: bool = True,
                         include_tech: bool = True) -> Optional[str]:
        """Return full aliased column name.

        E.g. `col_name as Column_Name`.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (str): aliased name

        """
        # Decide if value should be returned
        if self._should_return(col_type, filter_ignored, include_tech):
            return (f'{self._get_value_part()}'
                    f' AS {self.get_name()}')
        return None

    def get_def(self, col_type: str = 'all', filter_ignored: bool = True,
                include_tech: bool = True,
                supported_types: Union[List[str], None] = None) -> Optional[str]:
        """Return column sql definition.

        E.g. `col_name VARCHAR NOT NULL`.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (str): sql column definition

        """
        if self._should_return(col_type, filter_ignored, include_tech):
            return (f'{self.get_name()}'
                    f' {self._get_type(supported_types=supported_types)}'
                    f'{self._get_mandatory_def()}')
        return None

    def get_coalesce(self, col_type: str = 'all', filter_ignored: bool = True,
                     include_tech: bool = True) -> Optional[str]:
        """Return coalesce of two columns.

        E.g. `COALESCE(t.col_name, s.col_name) AS col_name`.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (str): sql column definition

        """
        if self._should_return(col_type, filter_ignored, include_tech):
            if self.pk:
                return f'COALESCE(s.{self.get_name()}, t.{self.get_name()}) ' \
                       f'AS {self.get_name()}'
            return f'CASE WHEN s.DW_LOAD_DATE IS NOT NULL THEN s.{self.get_name()} ' \
                   f'ELSE t.{self.get_name()} END AS {self.get_name()}'
        return None

    def get_join(self, col_type: str = 'all', filter_ignored: bool = True,
                 include_tech: bool = True) -> Optional[str]:
        """Get join condition of tables s and t.

        E.g. `s.col_name = t.col_name`.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (str): sql join

        """
        if self._should_return(col_type, filter_ignored, include_tech):
            return f's.{self.get_name()} = t.{self.get_name()}'
        return None

    def get_embulk_column_option(self, col_type: str = 'all', filter_ignored: bool = True,
                                 include_tech: bool = True) -> Optional[str]:
        """Get column option for Embulk.

        E.g. `col_name {value_type: string}`.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (str): embulk type

        """
        # Decide if value should be returned
        if self._should_return(col_type, filter_ignored, include_tech):
            return f'{self.get_name()}: {{value_type: {self._embulk_column_mapper()}}}'
        return None

    def _get_value_part(self, col_type: str = 'all', filter_ignored: bool = True,
                        include_tech: bool = True) -> Optional[str]:
        """Return value part. It is column name or constant. Used in aliasing: 4 AS COLUMN_NAME."""
        value = self.default_value or self.name.upper()
        # Decide if value should be returned
        if self._should_return(col_type, filter_ignored, include_tech):
            escape_symbol = '`' if self.escape else ''
            return f'{escape_symbol}{value}{escape_symbol}'
        return None

    def _get_clean_data_type(self):
        """Return data type without bracket. E.g. string(10) will be just string."""
        return self.data_type.split('(')[0]

    def _get_type(self, col_type: str = 'all', filter_ignored: bool = True,
                  include_tech: bool = True, without_length: bool = True,
                  supported_types: Union[List[str], None] = None) -> Optional[str]:
        """Return column type if it is valid data type."""
        supported_types = supported_types or []
        if self._should_return(col_type, filter_ignored, include_tech):
            clean_type = self._get_clean_data_type().upper()
            if clean_type in supported_types:
                if without_length:
                    return clean_type.upper()
                else:
                    return self.data_type.upper()
            raise TypeError(
                f'Column type `{clean_type}` is not supported data type.')
        return None

    def _get_mandatory_def(self, col_type: str = 'all', filter_ignored: bool = True,
                           include_tech: bool = True) -> Optional[str]:
        """Return mandatory definition."""
        if self._should_return(col_type, filter_ignored, include_tech):
            return ' NOT NULL' if self.mandatory else ''
        return None

    def _embulk_column_mapper(self) -> str:
        """Map SQL data type to Embulk.

        Returns:
            (str): Embulk data type

        """
        clean_type = self._get_clean_data_type().lower()
        try:
            # Decide if value should be returned
            return EMBULK_TYPE_MAPPER[clean_type]
        except KeyError:
            raise TypeError(
                f'Column type `{clean_type}` does not exists in column_type_mapper!')

    # pylint: disable=R0911
    def _should_return(self, col_type: str = 'all', filter_ignored: bool = True,
                       include_tech: bool = True) -> bool:
        """Decice if column should be returned.

        Parameters:
            col_type (str): what type of columns should be returned. Default `all`.
                Values:
                    - all - primary and nonprimary keys are returned
                    - pk - only primary keys are returned
                    - nonpk - only nonprimary keys are returned
            filter_ignored (bool): wheter ignored column should be filtered out from result.
                Default True.
            include_tech (bool): wheter technical columns should be included in result. Columns
                prefixed with DW_.

        Returns:
            (bool): wheter column should be returned

        """
        if include_tech and self.tech_column:
            if col_type == 'pk' and not self.pk:
                return False
            if col_type == 'nonpk' and self.pk:
                return False
            if filter_ignored and self.ignored:
                return False
            return True
        if not include_tech and self.tech_column:
            return False
        if col_type == 'pk' and not self.pk:
            return False
        if col_type == 'nonpk' and self.pk:
            return False
        if filter_ignored and self.ignored:
            return False
        return True
