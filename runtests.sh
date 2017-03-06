#!/usr/bin/env bash

nosetests dynamite --with-coverage --cover-package=dynamite --cover-erase $@
flake8 dynamite --ignore=E128,E501

if [[ ! "$@" ]] ; then
    echo
    coverage report
fi