BASEDIR=$(dirname $0)
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$BASEDIR/build/libs/test-yarn-0.1.jar:$BASEDIR/build/dependant-libs/scala-library-2.10.2.jar"
hadoop example.TestClient $@
