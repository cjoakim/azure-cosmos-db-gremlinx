# Windows PowerShell script to execute the export process.
# Chris Joakim, Microsoft

New-Item -ItemType Directory -Force -Path app\exports
New-Item -ItemType Directory -Force -Path app\tmp

del app\exports\*.*
del app\tmp\*.*

gradle checkEnv

gradle exportGremlinViaSqlEndpoint
