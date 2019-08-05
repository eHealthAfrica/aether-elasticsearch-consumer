from typing import Any, List, Mapping, Union

from app.logger import get_logger

LOG = get_logger('SCHEMA')


class Node:

    doc: str
    name: str
    type: Union[str, List[Any]]
    namespace: str
    __extended_type: str
    __lookup: List[Mapping[str, str]]
    has_children: bool
    children: Mapping[str, Any]

    def __init__(self, source, optional=False):
        self.has_children = False
        self.children = {}
        self.parse(source)
        self.parse_children(source)

    def __swap_name(self, name):
        ''' we can't have python attributes that start with @aether
            so we make a simple substitution
        '''
        if name.startswith('__'):
            return f'@aether_{name.lstrip("__")}'
        return name

    def parse(self, source):
        fields = [
            'doc',
            'name',
            'namespace',
            'default',
            '__extended_type',
            '__lookup'
        ]
        fields = {f: self.__swap_name(f) for f in fields}
        for field, alias in fields.items():
            if source.get(alias):
                setattr(self, field, source.get(alias))

    def parse_children(self, source):
        fields = source.get('fields', [])
        for f in fields:
            self.has_children = True
            name = f.get('name')
            grand_children = [i for i in f.get('type', []) if isinstance(i, dict)]
            if not grand_children:
                if not name:  # array values, ignore them
                    continue
                self.children[name] = Node(f)
            else:
                for gc in grand_children:
                    name = gc.get('name')
                    if not name:  # array values, ignore them
                        continue
                    self.children[name] = Node(gc)

    def iter_children(self, parent=''):
        lineage = f'{parent}.{self.name}' if parent else self.name
        if not self.has_children:
            yield lineage
        else:
            for key, child in self.children.items():
                for i in child.iter_children(lineage):
                    yield i

    def test_node(self, conditions):
        for attr in conditions.get('has_attr', []):
            if getattr(self, attr, None):
                return True
        for condition in conditions.get('match_attr', []):
            for k, v in condition.items():
                if getattr(self, k, None) == v:
                    return True
        return False

    def find_children(self, conditions, parent=''):
        lineage = f'{parent}.{self.name}' if parent else self.name
        if self.test_node(conditions):
            yield lineage
        if self.has_children:
            for child in self.children.values():
                for i in child.find_children(conditions, parent=lineage):
                    if i:
                        yield i

    def get_node(self, path):
        path_parts = path.split('.')
        if len(path_parts) == 1 and path == self.name:
            return self
        if len(path_parts) > 1 and path_parts[0] == self.name:
            if self.has_children:
                try:
                    _next = self.children[path_parts[1]]
                    return _next.get_node('.'.join(path_parts[1:]))
                except KeyError:
                    pass
        raise ValueError(f'No node found, deadend @ path {path}')

    def collect_matching(self, conditions):
        for path in self.find_children(conditions):
            yield (path, self.get_node(path))
