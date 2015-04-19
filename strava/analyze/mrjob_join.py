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




class MRJoin(MRJob):
    def mapper(self, _, line):
        try:
            if len(line.split(','))==12:
                #leaderboard rec
                wban = line.split(',')[2].replace('"','')
                date = line.split(',')[11].replace('"','')
                key = wban+'_'+date[:4]+date[5:7]+date[8:10]+'_'+date[11:13]
                output = line.split(',')
                for e in output:
                    e.replace('\n','')
                yield(key,output)

            if len(line.split(','))==7:
                #weather rec
                key = line.split(',')[0][:-2].replace('"','')
                output = line.split(',')
                for e in output:
                    e.replace('\n','')
                yield(key,output)
        except:
            #if an error occurs, move onto the next line
            pass

    def reducer(self, key, outputs):
        weather = None
        lists = list(outputs)
        for output in lists:
            if len(output) == 7:
                weather = output

        for output in lists:
            temp = weather
            if len(output) == 12:
                try:
                    out_list = temp + output
                    val = ','.join(out_list)
                    yield ('', val)
                except:
                    pass




if __name__ == '__main__':
    MRJoin.run()
