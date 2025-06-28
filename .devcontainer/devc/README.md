# Devcontainer Devcontainer

THis is a devcontainer for developing another devcontainers.

It's handy when you want to run a agentic loop on a new devcontainer, because this allows you to test-build the devcontainer without manually reopening the folder within the modified devcontainer:

```
$ mkdir -p test-workspace-folder; devcontainer down --config .devcontainer/tikv/devcontainer.json --workspace-folder test-workspace-folder
```

```
$ docker ps
CONTAINER ID   IMAGE                                                                                            COMMAND                  CREATED         STATUS         PORTS     NAMES
129fc2d7bebc   vsc-test-workspace-folder-d8ed634c45efec7af7afcb16ef78619b0cd560ef3803751ae54ec236c1b12434-uid   "/bin/sh -c 'echo Co…"   4 minutes ago   Up 4 minutes             determined_agnesi
```

```
$ docker rm -f determined_agnesi
determined_agnesi
```
