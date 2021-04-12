import inspect
import sys

import apache_beam as beam

from . import {{cookiecutter.plugin_module}}_inner as ext_mod


def dofn_init(self, *args):
    self.args = args

def dofn_process(self, element):
    inner = get_inner(self)
    return inner.process(element)

def get_inner(self):
    for name, obj in inspect.getmembers(ext_mod):
        if name == '__all__':
            for mem in obj:
                target = getattr(ext_mod, mem)
                if type(target) == type: 
                    if target.__name__ == self.__class__.__name__:
                        return target(*self.args)
 
for name, obj in inspect.getmembers(ext_mod):
    if name == '__all__':
        for mem in obj:
            target = getattr(ext_mod, mem)
            if type(target) == type: 
                if ext_mod.DoFnMark in target.__bases__:
                    py_class = type(target.__name__, (beam.DoFn,), {
                        '__init__': dofn_init,
                        'process': dofn_process,
                    })
                    setattr(sys.modules[__name__], target.__name__, py_class)

from . import {{cookiecutter.plugin_module}}

plugin = {{cookiecutter.plugin_module}}.{{cookiecutter.plugin_class}}
