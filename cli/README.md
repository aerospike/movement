The CLI is a simple portable interface that requires no extra dependencies.

```
$ java -jar cli/target/cli-1.0.0-SNAPSHOT.jar -h
Movement, by Aerospike.

Usage: <main class> [-h] [--list-components] [--list-tasks] [-c=<configPath>]
                    [task=<taskName>] [-s=<String=String>]...
  -c, --config=<configPath>
                          Path or URL to the configuration file
  -h, --help              Help
      --list-components   List available components
      --list-tasks        List available tasks
  -s, --set=<String=String>
                          Set or override configuration key
      task=<taskName>     Task to run
```

