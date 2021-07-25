For the latest information about Hadoop, please visit our website at:

   http://hadoop.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/HADOOP/

echo "# hadoop-source" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin git@github.com:SwordfallYeung/hadoop-source.git
git push -u origin main

Hadoop的包功能分析
hadoop-main                     所有工程的父工程
hadoop-assemblies
hadoop-build-tools
hadoop-client-modules
hadoop-cloud-storage-project
hadoop-common-project
hadoop-dist
hadoop-hdfs-project
hadoop-mapreduce-project
hadoop-maven-plugins
hadoop-minicluster
hadoop-project
hadoop-project-dist
hadoop-tools
hadoop-yarn-project
