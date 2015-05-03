#!/usr/bin/python
# Copyright 2009-2010 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The classic MapReduce job: count the frequency of words.
"""
from mrjob.job import MRJob
import re
import sys
import glob
import os




class MRMaxSpeed(MRJob):
    def mapper(self, _, line):
        key = line.split(',')[0]
        try:
            speed = (float(line.split(',')[3])*3600)/(float(line.split(',')[6])*1609.34)
            yield(key,speed)
        except Exception as e:
            #no valid speed
            pass

    def reducer(self, key, outputs):
        maxSpeed=0
        for speed in outputs:
            if speed>maxSpeed:
                maxSpeed=speed
        try:
            yield (key, maxSpeed)
        except Exception as e:
            pass




if __name__ == '__main__':
    MRMaxSpeed.run()
