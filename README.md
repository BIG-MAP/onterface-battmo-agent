# Onterface

GitRepo for Onterface (Ontology Enabled Data Interface) - a subgrant of [BIG-MAP](https://www.big-map.eu/)

see also: [Onterface Plattform](https://onterface.open-semantic-lab.org/wiki/)

## Related Projects
* Simulation Framework: https://github.com/BattMoTeam/BattMo
* Simulation Server: https://github.com/OpenBattTools/BattMo-Server
* Semantic Dataspace: https://github.com/OpenSemanticLab

## Development
In a single-user-environment like jupyterlab, please set authorship per commit:
```
export GIT_AUTHOR_NAME="YourGitUserName" && export GIT_AUTHOR_EMAIL="YourGitAccountEmail" && export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME" && export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL" && git commit -m "my commit msg"
```
e.g.
git add README.md
export GIT_AUTHOR_NAME="SimonStier" && export GIT_AUTHOR_EMAIL="simon.stier@isc.fraunhofer.de" && export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME" && export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL" && git commit -m "add readme"
git push origin main
