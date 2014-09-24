'''
Created on Sep 18, 2010

@author: haonguyen
'''
#!/usr/bin/python
import sys, os;
#Setting up environment
basedir = os.path.dirname(os.path.abspath(os.path.realpath(__file__)));
real_path = os.path.normpath(os.path.join(basedir,'../../..'))
os.environ['DJANGO_SETTINGS_MODULE'] = 'lunex.topup.admin.settings'
sys.path.append('D:/WORKSPACE/vnumbersvc/lunex/vnumbersvc')

#server
# sys.path.append('D:/WORKSPACE/vnumbersvc/lunex/vnumbersvc')

sys.path.append(os.path.abspath(real_path))


