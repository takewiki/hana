# conn for hana--------
#' 获取hana数据库链接
#'
#' @param ip 地址
#' @param port 端口号
#' @param db_name 数据库名称
#' @param user_name 用户名
#' @param pwd 密码
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples
#' hana_conn()
hana_conn <- function(ip="192.168.2.160",
                      port="31013",
                      db_name="WP1",
                      user_name="HD_REMOTE",
                      pwd ="Qq1234567890") {
  driver <- JDBC(driverClass = "com.sap.db.jdbc.Driver",
                 classPath = "/opt/jdbc/ngdbc.jar", identifier.quote = "'")
  conn_str <- paste0("jdbc:sap://",ip,":",port,"/?databaseName=",db_name)
  print(conn_str)
  res <- dbConnect( driver,conn_str,user_name, pwd)
  return(res)
}



#' 清空hana表中所有记录
#'
#' @param var_hana_conn 连接
#' @param table_name 表名
#'
#' @return 无
#' @import RJDBC
#' @export
#'
#' @examples
#' hana_truncTable()
hana_truncateTable <- function(var_hana_conn=hana_conn(),table_name="t_nka_fact") {
  sql_str <- paste0("truncate table  ",table_name)
  dbSendUpdate(var_hana_conn,sql_str)

}




#' 查询数据
#'
#' @param var_hana_conn 连接
#' @param sql_str SQL表达式
#'
#' @return 返回结果
#' @import RJDBC
#' @export
#'
#' @examples
#' hana_select()
hana_select <- function(var_hana_conn=hana_conn(),sql_str) {
  res <- dbGetQuery(var_hana_conn,sql_str)
  return(res)

}


#' 测试数据数据
#'
#' @param var_hana_conn 连接
#' @param sql_str 字符串
#'
#' @return 返回值
#' @export
#'
#' @examples
#' hana_update()
hana_update <- function(var_hana_conn=hana_conn(),sql_str) {

  RJDBC::dbSendUpdate(conn,sql_str);

}

#' 返回表的行数
#'
#' @param var_hana_conn  连接
#' @param table_name  表名
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples
#' hana_tableCount()
hana_tableCount <- function(var_hana_conn=hana_conn(),table_name) {

  sql <- paste0("select  count(1)  as FCount   from  ",table_name)
  r<- hana_select(var_hana_conn = var_hana_conn,sql_str = sql)
  res <- r$FCount
  return(res)

}


#' hana写入数据库
#'
#' @param var_hana_conn 连接
#' @param table_name 表名
#' @param r_object 对话
#' @param append 是否附加
#'
#' @return 返回值
#' @export
#'
#' @examples
#' hana_writeTable()
hana_writeTable <- function(var_hana_conn=hana_conn(),table_name,r_object,append=FALSE){

  if (append == FALSE){
    res<- dbWriteTable(var_hana_conn, table_name, r_object)
  }else{
    res<- dbWriteTable(var_hana_conn, table_name, r_object,append=T, row.names=F, overwrite=F)
  }

  return(res)

}



#' 分页查询
#'
#' @param var_hana_conn 连接
#' @param sql_str 基本语句
#' @param page_by 分页字段
#' @param from 开始
#' @param to  结束
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples()
#' hana_select_paging
hana_select_paging <- function(var_hana_conn=hana_conn(),
                               sql_str,
                               page_by='fid',
                               from=1,
                               to =10000
                               ) {
  sql_str2 <- paste0(sql_str," where ",page_by," >= ",from,"  and ",page_by," <=  ",to)
  print(sql_str2)
  res <- dbGetQuery(var_hana_conn,sql_str2)
  return(res)



}


#' hana数据上传
#'
#' @param var_hana_conn 连接
#' @param data 数据
#' @param table_name 表名
#' @param page_count 页面
#' @param sleep_second 间隔时间
#'
#' @return 无返回值
#' @import RJDBC
#' @export
#'
#' @examples
#' hana_upload()
hana_upload_paging <- function(var_hana_conn=hana_conn(),data,table_name,page_count =5000,sleep_second=3) {
  ncount <- nrow(data)

  pages_info <- tsdo::paging_setting(ncount,page_count)
  ncount_page <- nrow(pages_info)
  lapply(1:ncount_page, function(page_index){
    FStart <- pages_info$FStart[page_index]
    FEnd <-  pages_info$FEnd[page_index]
    RJDBC::dbWriteTable(var_hana_conn,table_name, data[FStart:FEnd,],append=T, row.names=F, overwrite=F)
    print(paste0('step',page_index,' from ',FStart,' to ',FEnd))
    Sys.sleep(sleep_second)

  })

}


#' 同步sap hana数据
#'
#' @param var_hana_conn 连接
#' @param sql_str 基本SQL
#' @param table_name_src 来源表
#' @param table_name_target 目标表
#' @param page_by 分页字段
#' @param query_pageCount 查询批次
#' @param upload_pageCount 写入批次
#' @param sleep_second 写入间隔
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples
#' hana_syncData()
hana_syncData <- function(var_hana_conn=hana_conn(),
                          var_sql_conn,
                          sql_str,
                          table_name_src,
                          table_name_target,
                          page_by='fid',
                          query_pageCount=100000,
                          upload_pageCount=5000,
                          sleep_second=3
                          ) {
  #获取整表数量
  table_count <- sqlr::sql_tableCount(var_sql_conn,table_name = table_name_src)
  if (table_count >0){
     #计算查询批次
     page_info_query <- tsdo::paging_setting(table_count,query_pageCount)
     page_count_query <- nrow(page_info_query)
     lapply(1:page_count_query, function(query_index){
       from <- page_info_query$FStart[query_index]
       to <- page_info_query$FEnd[query_index]
       #获取每一次查询数据
       data_query<- sqlr::sql_select_paging(conn = var_sql_conn,sql_str = sql_str,page_by = page_by,from = from,to = to)
       #针对查询的数据进行写入
       hana_upload_paging(var_hana_conn = var_hana_conn,data = data_query,table_name =table_name_target,page_count = upload_pageCount,sleep_second =  sleep_second)
})



  }


}





