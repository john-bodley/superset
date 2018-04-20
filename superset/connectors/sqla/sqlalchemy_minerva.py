import logging
import os
import sqlparse
import sys

import git
from pyhive.sqlalchemy_presto import PrestoDialect
from sqlalchemy.sql.compiler import GenericTypeCompiler, SQLCompiler
from sqlalchemy.sql.selectable import Join
sys.path.append('/mnt/data/minerva/lib')
from minerva_parser.minerva import Minerva


minerva = Minerva(path='/mnt/data/minerva/configs')


def find(tokens, found=None):
    if found is None:
        found = {}
    for token in tokens:
        if token.is_group:
            find(token.tokens, found)
        elif token.ttype == sqlparse.tokens.Token.Name:
            try:
                ref = token.parent.parent.parent
                if isinstance(ref, sqlparse.sql.Function):
                    found[ref] = token.value
            except AttributeError:
                pass
    return found


class Function(object):
    """
    """


class Blah(object):
    def __init__(self, column):
        """
        :param column:
        :param select:
        """

        self.column = column
        self.statement = sqlparse.parse(str(column))[0]
        #self.tokens = list(self.statement.flatten())

        self.found = find(self.statement.tokens) if column != '__timestamp' else {}
        self.is_aggregate = bool(self.found)

    def mutate(self):
        for token in self.found.keys():
            #class(token) == sqlparse.sql.Identifier
            token.tokens = [sqlparse.sql.Token(sqlparse.tokens.Token.Name, '"{}"'.format(token.value))]



    @property
    def columns(self):
        return set(self.found.values())


class MinervaCompiler(SQLCompiler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        """
        select = statement.select

        if select._group_by_clause.clauses:
            group_by = select._group_by_clause._compiler_dispatch(self)
            print('group_by')
            print(group_by)

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self)
            print('t')
            print(t)
        """

    def get_metrics(self, select):
        metrics = set()
        print('get_metrics')
        for _, column in select._columns_plus_names:
            if column.name != '__timestamp':
                blah = Blah(column)
                metrics |= blah.columns
        print(metrics)
        return metrics

    def get_metrics2(self, select):
        metrics = set()

        print(select._order_by_clause, dir(select._order_by_clause))
        for column in select._order_by_clause:
            print('get_metrics2')
            print(str(column))
            if str(column) != '__timestamp':
                blah = Blah(column)
                metrics |= blah.columns

        return metrics

    def get_dimensions(self, select):
        if select._group_by_clause.clauses:
            return {str(x) for x in select._group_by_clause.clauses if x.name != '__timestamp'}

        return set()

    def visit_select(self, select, **kwargs):
        """
        """

        print('visit_select')
        print(select)
        print(kwargs)
        """
        for attr in dir(select.selectable):
            print('>>>>')
            print(attr)
            print(getattr(select, attr), type(getattr(select, attr)))
        """

        dimensions = self.get_dimensions(select)
        metrics = self.get_metrics(select)
        sources = get_sources(metrics, dimensions)
        print(dimensions)
        print(metrics)
        print(sources)
        import uuid


        views = dict()
        for source in sources.keys():
            views[source] = '"druid"."tmp_{}"'.format(str(uuid.uuid4()))

        columns = [Blah(column) for _, column in select._columns_plus_names]
        columns = sorted(columns, key=lambda column: column.is_aggregate)

        if sources:
            text = []
            for source, metrics in sources.items():
                text.append('CREATE OR REPLACE VIEW {} AS'.format(views[source]))
                text.append('SELECT')

                slct = []
                seen = set()
                for blah in columns:
                    print(blah, blah.is_aggregate)
                    if blah.is_aggregate:
                        for token, metric in blah.found.items():
                            if metric in metrics and token.value not in seen:
                                slct.append('{0} AS "{0}"'.format(token.value))
                                seen.add(token.value)
                    else:
                        slct.append('{} AS "{}"'.format(str(blah.column), blah.column.name))

                text.append(','.join(slct))
                text.append('FROM {}'.format('"druid"."minerva__{}"'.format(source)))

                if select._whereclause is not None:
                    t = select._whereclause._compiler_dispatch(self)
                    text.append('WHERE {}'.format(t))

                if select._group_by_clause.clauses:
                    group_by = select._group_by_clause._compiler_dispatch(self, **kwargs)

                    if group_by:
                        text.append('GROUP BY {}'.format(group_by))

                text.append(';')

            from_ = select.froms[0]
            sort_by_view = None
            dimensions2 = None
            if isinstance(from_, Join):
                select2 = from_.right.original
                dimensions2 = self.get_dimensions(select2)
                metrics2 = self.get_metrics2(select2)
                sources2 = get_sources(metrics2, dimensions2)

                if sources2:
                    source = next(iter(sources2.keys()))
                    sort_by_view = '"druid"."tmp_{}"'.format(str(uuid.uuid4()))
                    text.append('CREATE OR REPLACE VIEW {} AS'.format(sort_by_view))
                    text.append('SELECT')
                    text.append(','.join(['"{}"'.format(x) for x in dimensions2]))
                    text.append('FROM {}'.format('"druid"."minerva__{}"'.format(source)))

                    if select._whereclause is not None:
                        t = select._whereclause._compiler_dispatch(self)
                        text.append('WHERE {}'.format(t))
                    text.append('GROUP BY {}'.format(','.join(['"{}"'.format(x) for x in dimensions2])))
                    text.append('ORDER BY {}'.format(select2._order_by_clause))
                    if select2._limit_clause is not None:
                        text.append('LIMIT {}'.format(self.process(select2._limit_clause, **kwargs)))

                    text.append(';')

            if sort_by_view:
                text.append('SELECT')
                text.append('a.*')
                text.append('FROM (')

            text.append('SELECT')
            slcts = []
            views2 = list(views.keys())

            for blah in columns:
                if blah.is_aggregate:
                    blah.mutate()
                    prefix = str(blah.statement)
                else:
                    if len(sources) > 1:
                        prefix = 'COALESCE({})'.format(','.join(['{}."{}"'.format(t, blah.column.name) for t in views.values()]))
                    else:
                        prefix = '"{}"'.format(blah.column.name)

                slcts.append('{} AS "{}"'.format(prefix, blah.column.name))

            text.append(','.join(slcts))
            text.append('FROM')

            text.append(views[views2[0]])

            for idx in range(1, len(views2)):
                text.append('FULL OUTER JOIN')
                text.append(views[views2[idx]])
                text.append('ON')
                on_ = []
                for column in select._group_by_clause.clauses:
                    on_.append(
                        '{}."{}" = {}."{}"'.format(
                            views[views2[idx - 1]],
                            column.name,
                            views[views2[idx]],
                            column.name,
                        )
                    )

                text.append(' AND '.join(on_))

            if len(sources) > 1:
                text.append(' AND COALESCE({}) IS NOT NULL'.format(','.join(['{}."{}"'.format(str(column)) for column in select._group_by_clause.clauses])))

            if sort_by_view:
                text.append(') a')
                text.append('JOIN {} b'.format(sort_by_view))
                text.append('ON')
                on_ = []
                for column in dimensions2:
                    on_.append('"a"."{0}" = "b"."{0}"'.format(column))
                text.append(' AND '.join(on_))

                if select._limit_clause is not None:
                    text.append('LIMIT {}'.format(self.process(select._limit_clause, **kwargs)))

            return '\n'.join(text)
        else:
            raise Exception()


class MinervaTypeCompiler(GenericTypeCompiler):
    def process(self, type_, **kw):
        pass


class MinervaDialect(PrestoDialect):
    name = 'minerva'
    statement_compiler = MinervaCompiler


def get_conf_name(team, metrics):
    """
    Generate a conf name
    """
    return '{}.{}'.format(team, metrics[0])


def get_sources(metrics, dimensions):
    """
    """

    from collections import defaultdict
    sources = defaultdict(set)

    for metric in metrics:
        if metric not in minerva.metrics:
            return

        source = minerva.metrics[metric].source

        if dimensions <= set(source.dimensions.keys()):
            sources[source.name].add(metric)
        else:
            for name, ds in source.dimension_sets.items():
                if dimensions <= {d[1] for d in ds.get_dimension_list()}:
                    sources['{}__{}'.format(source.name, name)].add(metric)
                    break
            else:
                return

    return sources
