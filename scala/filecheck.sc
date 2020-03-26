
import scala.io.Source._
val t = fromFile("C:\\Users\\User\\IdeaProjects\\untitled\\datasource\\avia\\airports-extended.dat.txt")
for(line <- t.getLines().toString){
  print(line)
}
t.close()
