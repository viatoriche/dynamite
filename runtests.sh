#!/usr/bin/env bash

AWS_ACCESS_KEY_ID=123 AWS_SECRET_ACCESS_KEY=123 nosetests dynamite --with-coverage --cover-package=dynamite --cover-erase $@
flake8 dynamite --ignore=E128,E501

if [[ ! "$@" ]] ; then
    echo
    coverage report
fi