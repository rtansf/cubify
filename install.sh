#!/bin/bash
mongod --smallfiles &> /tmp/mongod.log &
cd cubify
sudo python setup.py install