import os

import jpype

print('mark 1')

print('JAVA_HOME:', os.environ.get('JAVA_HOME', '(not set)'))
print('jpype.getDefaultJVMPath():', jpype.getDefaultJVMPath())
print('jpype.getClassPath():', jpype.getClassPath())

jpype.startJVM()

print('mark 2')
