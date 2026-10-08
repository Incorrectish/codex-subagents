"""Inspect the installed app-server schema without starting a model turn."""
from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

RESPONSE_TYPES = {
    'item/commandExecution/requestApproval': 'CommandExecutionRequestApprovalResponse',
    'item/fileChange/requestApproval': 'FileChangeRequestApprovalResponse',
    'item/permissions/requestApproval': 'PermissionsRequestApprovalResponse',
    'item/tool/requestUserInput': 'ToolRequestUserInputResponse',
    'mcpServer/elicitation/request': 'McpServerElicitationRequestResponse',
    'applyPatchApproval': 'ApplyPatchApprovalResponse',
    'execCommandApproval': 'ExecCommandApprovalResponse',
}


def read_bundle(root):
    requests = json.loads((root/'ClientRequest.json').read_text())
    methods = {}
    for variant in requests['oneOf']:
        fields = variant.get('properties',{})
        for method in fields.get('method',{}).get('enum',[]):
            ref = fields.get('params',{}).get('$ref','').split('/')[-1]
            methods[method] = requests.get('definitions',{}).get(ref,{})
    responses = {}
    for method, name in RESPONSE_TYPES.items():
        for path in (root/'v2'/(name+'.json'), root/(name+'.json')):
            if path.exists():
                responses[method] = json.loads(path.read_text())
                break
    return {'methods':methods,'responses':responses}


def inspect_local():
    with tempfile.TemporaryDirectory(prefix='codex-subagents-schema-') as directory:
        subprocess.run(['codex','app-server','generate-json-schema','--experimental','--out',directory],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        return read_bundle(Path(directory))


def validate(value,schema,root=None):
    """Validate the JSON Schema subset generated for native request responses.

    Unknown assertion keywords fail closed rather than silently accepting a
    newer response contract. An installed schema is data, never executable code.
    """
    if schema is True: return
    if schema is False: raise ValueError("answer refused by native schema")
    root = root or schema
    supported = {'$schema','$id','$ref','definitions','title','description','default','deprecated','examples',
                 'type','enum','const','properties','required','additionalProperties','items','anyOf','oneOf','allOf',
                 'minimum','maximum','minItems','maxItems','minLength','maxLength','format'}
    if set(schema)-supported:
        raise ValueError('unsupported response schema constraints')
    if '$ref' in schema:
        if not schema['$ref'].startswith('#/definitions/'):
            raise ValueError('external schema references are unsupported')
        return validate(value,root['definitions'][schema['$ref'].split('/')[-1]],root)
    for key in ('anyOf','oneOf'):
        if key in schema:
            matches = 0
            for child in schema[key]:
                try: validate(value,child,root); matches += 1
                except ValueError: pass
            if not matches or key=='oneOf' and matches!=1:
                raise ValueError('answer does not match native response schema')
    for child in schema.get('allOf',[]): validate(value,child,root)
    types = schema.get('type',[])
    types = [types] if isinstance(types,str) else types
    checks = {'null':value is None,'boolean':isinstance(value,bool),'string':isinstance(value,str),
              'integer':isinstance(value,int) and not isinstance(value,bool),
              'number':isinstance(value,(int,float)) and not isinstance(value,bool),
              'object':isinstance(value,dict),'array':isinstance(value,list)}
    if types and not any(checks.get(t,False) for t in types): raise ValueError('invalid answer type')
    if 'enum' in schema and value not in schema['enum']: raise ValueError('invalid answer choice')
    if 'const' in schema and value!=schema['const']: raise ValueError('invalid constant')
    if isinstance(value,dict):
        if set(schema.get('required',[]))-set(value): raise ValueError('missing answer fields')
        props = schema.get('properties',{})
        extra = schema.get('additionalProperties',True)
        for key,item in value.items():
            if key in props: validate(item,props[key],root)
            elif extra is False: raise ValueError('unexpected answer field')
            elif isinstance(extra,dict): validate(item,extra,root)
    if isinstance(value,list):
        for item in value: validate(item,schema.get('items',{}),root)
    for keyword,cmp in (('minimum',lambda a,b:a>=b),('maximum',lambda a,b:a<=b)):
        if keyword in schema and not cmp(value,schema[keyword]): raise ValueError('answer outside schema range')
    for keyword,cmp in (('minItems',lambda a,b:len(a)>=b),('maxItems',lambda a,b:len(a)<=b),('minLength',lambda a,b:len(a)>=b),('maxLength',lambda a,b:len(a)<=b)):
        if keyword in schema and not cmp(value,schema[keyword]): raise ValueError('answer outside schema size')
