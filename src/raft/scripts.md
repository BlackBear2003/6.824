## run with pipenv

```shell
VERBOSE=1 go test -run 2A -race | pipenv run python3 dslogs.py -c 7
```

## large number testing

```shell
python3 dstest.py 2A 2B -n 1000 -v   
```

## simple test

```shell
go test -run 2A  
```