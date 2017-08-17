

case class Tree(indent:Int, text:String)
{

    var children: Seq[Tree] = List[Tree]()
    var parent: Option[Tree] = None

    def addChild(paramIndent: Int, paramText: String): Unit = {
        assert(paramIndent > indent)
        var newChild = new Tree(paramIndent, paramText)
        newChild.parent = Some(this)
        children = children :+ newChild
    }

    def getRecentChild: Tree = {
        children(children.size - 1)
    }

    def getTopNode(indent: Int): Tree = {
        var currentNode = this
        while (currentNode.indent >= indent && !currentNode.parent.isEmpty)
            currentNode = currentNode.parent.get
        currentNode
    }

    def toString(paramResult: String): String = {
        var result = paramResult
        result = result + " " * indent + text + "\n"
        for (child <- children)
            result = child.toString(result)
        result
    }

    def toJson(paramResult: String): String = {
        var result = """{"name":"""" + text +"""""""
        if (children.size > 0)
        {
            result = result + """, "children":[""" + children.map(x=>x.toJson(result)).mkString(",") + "]"
            /*for (i <- 0 to children.size)
            {
                result = children(i).toJson(result)
                if (i < children.size) result = result + ","
            }*/
            /*result = result + "]"*/
        }
        result + "}"
    }

}

/*
var x = Tree(0, "root")
x.addChild(1, "left")
x.addChild(1, "right")
var y = x.children(0)
y.addChild(2, "left")
y.addChild(2, "right")
*/


    
    val root = new Tree(-1, "Plan")
    var currentTree = root

    val plan = """*Project [uid#144, booking_ticket_id#2L, airport_origin#5, airport_destination#4, date_departure_planned#143, operating_carrier_code#48]
    +- *SortMergeJoin [booking_ticket_id#2L], [booking_ticket_id#4658L], Inner
    :- *Sort [booking_ticket_id#2L ASC], false, 0
    :  +- Exchange hashpartitioning(booking_ticket_id#2L, 200)
    :     +- *Project [uid#144, AIRPORT_ORIGIN#5, AIRPORT_DESTINATION#4, date_departure_planned#143, operating_carrier_code#48, booking_ticket_id#2L]
    :        +- *Filter isnotnull(booking_ticket_id#2L)
    :           +- *Scan csv [booking_ticket_id#2L,AIRPORT_DESTINATION#4,AIRPORT_ORIGIN#5,OPERATING_CARRIER_CODE#48,DATE_DEPARTURE_PLANNED#143,UID#144] Format: CSV, InputPaths: hdfs://nameservice1/data/19777915-32a0-4b18-8720-7063fca39c0b/model/stage_1/ticket1502374991171_old, PartitionFilters: [], PushedFilters: [IsNotNull(booking_ticket_id)], ReadSchema: struct<booking_ticket_id:bigint,AIRPORT_DESTINATION:string,AIRPORT_ORIGIN:string,OPERATING_CARRIE...
    +- *Project [booking_ticket_id#4658L]
        +- *Filter (isnotnull(rn#3869) && (rn#3869 = 1))
            +- Window [rownumber() windowspecdefinition(booking_ticket_id#4658L, DATE_DEPARTURE_PLANNED#4799 ASC, ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn#3869], [booking_ticket_id#4658L], [DATE_DEPARTURE_PLANNED#4799 ASC]
                +- *Sort [booking_ticket_id#4658L ASC, DATE_DEPARTURE_PLANNED#4799 ASC], false, 0
                +- *Project [booking_ticket_id#4658L, DATE_DEPARTURE_PLANNED#4799]
                    +- *SortMergeJoin [booking_ticket_id#4658L], [booking_ticket_id#4511L], Inner
                        :- *Sort [booking_ticket_id#4658L ASC], false, 0
                        :  +- Exchange hashpartitioning(booking_ticket_id#4658L, 200)
                        :     +- *Project [DATE_DEPARTURE_PLANNED#4799, booking_ticket_id#4658L]
                        :        +- *Filter isnotnull(booking_ticket_id#4658L)
                        :           +- *Scan csv [booking_ticket_id#4658L,DATE_DEPARTURE_PLANNED#4799] Format: CSV, InputPaths: hdfs://nameservice1/data/19777915-32a0-4b18-8720-7063fca39c0b/model/stage_1/ticket1502374991171_old, PartitionFilters: [], PushedFilters: [IsNotNull(booking_ticket_id)], ReadSchema: struct<booking_ticket_id:bigint,DATE_DEPARTURE_PLANNED:timestamp>
                        +- *Project [booking_ticket_id#4511L]
                            +- *Filter ((isnotnull(min(OPERATING_CARRIER_CODE#48)#4508) && (count(distinct OPERATING_CARRIER_CODE#48)#4507L = 1)) && (min(OPERATING_CARRIER_CODE#48)#4508 = S7))
                            +- SortAggregate(key=[booking_ticket_id#4511L], functions=[min(OPERATING_CARRIER_CODE#4557),count(distinct OPERATING_CARRIER_CODE#4557)])
                                +- *Sort [booking_ticket_id#4511L ASC], false, 0
                                    +- Exchange hashpartitioning(booking_ticket_id#4511L, 200)
                                        +- SortAggregate(key=[booking_ticket_id#4511L], functions=[merge_min(OPERATING_CARRIER_CODE#4557),partial_count(distinct OPERATING_CARRIER_CODE#4557)])
                                        +- SortAggregate(key=[booking_ticket_id#4511L,OPERATING_CARRIER_CODE#4557], functions=[merge_min(OPERATING_CARRIER_CODE#4557)])
                                            +- *Sort [booking_ticket_id#4511L ASC, OPERATING_CARRIER_CODE#4557 ASC], false, 0
                                                +- Exchange hashpartitioning(booking_ticket_id#4511L, OPERATING_CARRIER_CODE#4557, 200)
                                                    +- SortAggregate(key=[booking_ticket_id#4511L,OPERATING_CARRIER_CODE#4557], functions=[partial_min(OPERATING_CARRIER_CODE#4557)])
                                                    +- *Sort [booking_ticket_id#4511L ASC, OPERATING_CARRIER_CODE#4557 ASC], false, 0
                                                        +- *Project [OPERATING_CARRIER_CODE#4557, booking_ticket_id#4511L]
                                                            +- *Filter isnotnull(booking_ticket_id#4511L)
                                                                +- *Scan csv [booking_ticket_id#4511L,OPERATING_CARRIER_CODE#4557] Format: CSV, InputPaths: hdfs://nameservice1/data/19777915-32a0-4b18-8720-7063fca39c0b/model/stage_1/ticket1502374991171_old, PartitionFilters: [], PushedFilters: [IsNotNull(booking_ticket_id)], ReadSchema: struct<booking_ticket_id:bigint,OPERATING_CARRIER_CODE:string>
"""

    val modifiedPlan = plan.replaceAll("[//*+-:]","")

    def getIndent(line: String): Int = {
     var i = 0
        while (line(i) == ' ')
            i = i + 1
        i
    }

    def getText(line: String): String = {
        line.substring(getIndent(line)).split("[ (]")(0)
    }

    def parsePlan(text: String, paramCurrentTree: Tree): Tree = {
        val lines = text.split("\n")
        var currentTree = paramCurrentTree
        for(line <- lines)
        {
            
            val currentIndent = getIndent(line)
            val currentText = getText(line)

            println(currentIndent)
            println(currentText)
            println(currentIndent)

            currentTree = currentTree.getTopNode(currentIndent)
            currentTree.addChild(currentIndent, currentText)
            currentTree = currentTree.getRecentChild

        }

        currentTree.getTopNode(-1)

    }
    

    val t = parsePlan(modifiedPlan, currentTree)
    println(t.toJson(""))

