#include <unistd.h>
#include <stdio.h>     
#include <sys/types.h>     
#include <sys/socket.h>     
#include <netinet/in.h>     
#include <arpa/inet.h>   
#include <stdlib.h>  
#include <string.h>  
#include <sys/epoll.h>  
#include <fcntl.h>
#include <sys/time.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdarg.h>
#include <assert.h>
#include <hiredis/hiredis.h>
#include <malloc.h>
 
#define BUFFER_SIZE 1024*512
#define MAX_EVENTS 10000
#define HALF_MAX_EVENTS 5000
#define MAX_TIME 60

char masterAip[64] = {0};
char masterAport[64] = {0};
char masterBip[64] = {0};
char masterBport[64] = {0};
char second[64] = {0};
char microsecond[64] = {0};

char *spaceptr[1024] = {0};
char tembuff[1024*512] = {0};


/**************read config************/
int ReadKeyData(char* pkey, int keyLen, char* pkeyData)
{
     int Len = keyLen;
     //FILE *fp = fopen("/etc/mosquitto/Proxy.conf", "r");
     FILE *fp = fopen("superclient.conf", "r");
     if(fp == NULL)
          return -1;
     char str[256] = {0};
     while(!feof(fp))
      {
          fgets(str, sizeof(str), fp);
          char objectKey[64] = {0};
          memcpy(objectKey, str, Len);
          if(strcmp(pkey, objectKey) == 0)
           {
                memcpy(pkeyData, str+Len+1, strlen(str)-Len-2);
                break;
           }
      }
    fclose(fp);
    return 0;
}

int ReadSuperClientConf()
{    
    ReadKeyData("masterAip", 9, masterAip);
    if (strlen(masterAip) == 0)
    {
        return -1;
    }
    ReadKeyData("masterAport", 11, masterAport);
    if (strlen(masterAport) == 0)
    {
        return -1;
    }
    ReadKeyData("masterBip", 9, masterBip);
    if (strlen(masterBip) == 0)
    {
        return -1;
    }
    ReadKeyData("masterBport", 11, masterBport);
    if (strlen(masterBport) == 0)
    {
        return -1;
    }
    ReadKeyData("second", 6, second);
    if(strlen(second) == 0)
    {
        return -1;
    }
    ReadKeyData("microsecond", 11, microsecond);
    if(strlen(microsecond) == 0)
    {
        return -1;
    }

    //DEBUG
    printf("%s\n", masterAip);
    printf("%s\n", masterAport);
    printf("%s\n", masterBip);
    printf("%s\n", masterBport);
    printf("%s\n", second);
    printf("%s\n", microsecond);
    
    return 0;
}
/****************************************/
int flag = 0;

struct sockaddr_in server_addr,client_addr;
char buff[BUFFER_SIZE] = {0};
char backbuf[BUFFER_SIZE] = {0}; 
char secbackbuf[BUFFER_SIZE] = {0};
redisContext *conn_a, *conn_b;


int SockInit(void)
{
    memset(&server_addr,0,sizeof(struct sockaddr_in));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr =htonl(INADDR_ANY);
    server_addr.sin_port = htons(8080);
    return 0;
}

int redisConnectFuc_a(void)
{
    //conn_a = redisConnect("192.168.12.124",6379);
    //conn_a = redisConnect(masterAip,atoi(masterAport));
    
    struct timeval timeout;
    timeout.tv_sec = (long)atoi(second);
    timeout.tv_usec = (long)atoi(microsecond);
    conn_a = redisConnectWithTimeout(masterAip,atoi(masterAport), timeout);

    if( conn_a != NULL && conn_a->err )
    {
        //redisFree(conn_a);
        printf("Connect to redisserver a failed\n");
        return -1;
    }
    printf("Connect to redisserver a successfully\n");
    return 0;
}

int redisConnectFuc_b(void)
{
    //conn_b = redisConnect("192.168.12.239",6379);
    //conn_b = redisConnect(masterBip,atoi(masterBport));
    struct timeval timeout;
    timeout.tv_sec = (long)atoi(second);
    timeout.tv_usec = (long)atoi(microsecond);
    conn_b = redisConnectWithTimeout(masterBip,atoi(masterBport), timeout);

    if( conn_b != NULL && conn_b->err )
    {
        //redisFree(conn_b);
        printf("Connect to redisserver b failed\n");
        return -1;
    }
    printf("Connect to redisserver b successfully\n");
    return 0;
}

int processCommandRecursion(char *buffer, int num)
{
    if(*(buffer+num) - 48 >= 9 ||  *(buffer+num) - 48 < 0)
    {
        return -1;
    }
    else
    {
        *(buffer+num) = ' ';
        processCommandRecursion(buffer, ++num);
    }
    return 0;
}

int processCommandFunc(char *tbuffer)
{
    int j = strlen(tbuffer);
    int k;
    int tmp = 0;
    for(k = 0; k < j; ++k)
    {
        if(tbuffer[k] == '\r' || tbuffer[k] == '\n')//zhuan yi zifu  \"    {"*3\r\n$3\r\nset\r\n$4\r\nkey\r\n$1\r\n1\0"}
        {
            ++tmp;
            tbuffer[k] = ' ';
            if(tmp == 12)
            {
                if( tbuffer[8] == 's' || tbuffer[8] == 'S')
                {
                    int i = k + 1;
                    int tp = k;
                    for(i;  i< j; ++i)
                    {
                        if(tbuffer[i] == ' ')
                        {
                            flag = 1;
                            strcpy(backbuf, tbuffer);
                            tbuffer[tp] = '\0';
                            backbuf[tp] = '.';
                            strcpy(secbackbuf, tbuffer);
                            sprintf(tbuffer, "%s%s", secbackbuf, "%s");
                            int t = 0;
                            char *temp = strchr(backbuf, '.');
                            ++temp;
                            while( *temp != backbuf[j-2])
                            {
                                    backbuf[t] = *temp;
                                    ++temp;
                                    ++t;
                            }
                            backbuf[t] = '\0';
                            break;
                        }
                    }
                }
            }
        }
    }
    char *newline = strchr(tbuffer, '*');
    int i = *(newline+1) - 48;
    if(i < 1 || i > 9)
    {
        return -1;
    }
    tbuffer[0] = ' ';
    tbuffer[1] = ' ';
    int temp;
    printf("The number i is: %d\n", i);
    for(temp = 0; temp < i; ++temp)
    {
        int recursion = 2;
        char  *newline = strchr(tbuffer, '$');
	printf("The newline is:%s\n", newline);
        *newline = ' ';
        *(newline+1) = ' ';
	processCommandRecursion(newline, recursion);
       //if (processCommandRecursion(newline, recursion) == -1)
       {
          // return -1;
       }
    }
    return 0;
}

int setnonblocking(int sockfd)
{
    if (fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFD, 0) | O_NONBLOCK) == -1)
        {
            return -1;
        }
        return 0;
}

int redis_func_a_plus(char *buffer, char *backbuf)
{       
        redisReply *rep = (redisReply*)redisCommand(conn_a, buff, backbuf);
        if ( rep == NULL )
        {
            printf("Execute command failed\n");
            redisFree(conn_a);
            return -1;
        }
        freeReplyObject(rep);
        printf("Succeed to execute command [%s]\n",buffer);
        return 0;
}

int redis_func_b_plus(char *buffer, char *backbuf)
{       
        redisReply *rep = (redisReply*)redisCommand(conn_b, buff, backbuf);
        if ( rep == NULL )
        {
            printf("Execute command failed\n");
            redisFree(conn_b);
            return -1;
        }
        freeReplyObject(rep);
        printf("Succeed to execute command [%s]\n",buffer);
        return 0;
}


int redis_func_a(char *buffer)
{
        redisReply *rep = (redisReply*)redisCommand(conn_a, buffer);
        if ( rep == NULL )
        {
            printf("Execute command   failed\n");
            redisFree(conn_a);
            return -1;
        }
        freeReplyObject(rep);
        printf("Succeed to execute command [%s]\n",buffer);
        return 0;
}

int redis_func_b(char *buffer)
{
        redisReply *rep = (redisReply*)redisCommand(conn_b, buffer);
        if ( rep == NULL )
        {
            printf("Execute command  failed\n");
            redisFree(conn_b);
            return -1;
        }
        freeReplyObject(rep);
        printf("Succeed to execute  command [%s]\n",buffer);
        return 0;
}


int main(int argc, char * argv[])     
{  
    int nRet = ReadSuperClientConf();
    if (nRet == 0)
    {
        printf("read config succ\n");
    }
    //return 1;
    struct linger so_linger;
    so_linger.l_onoff = 1;
    so_linger.l_linger = 0; 
    ///////////////////
    SockInit();   
    signal(SIGPIPE,SIG_IGN); 
    int listen_sock;//listen套接��? 
    int client_sock;   
    //int len = 0; 
    int rescue = 1;
    memset(buff,0,BUFFER_SIZE); // 数据传送的缓冲��? 
    listen_sock = socket(AF_INET,SOCK_STREAM,0);// 创建服务器端套接��?-IPv4协议，面向连接通信，TCP协议  
    setsockopt(listen_sock,SOL_SOCKET,SO_REUSEADDR,&rescue,sizeof(rescue));
   // 将套接字绑定到服务器的网络地址��?
    if (bind(listen_sock,(struct sockaddr *)(&server_addr),sizeof(struct sockaddr)) != 0)
    {     
        perror("bind");     
        return -1;     
    }   
    listen(listen_sock,128); // 监听连接请求--监听队列长度��?   
    printf("OK\n" );
    int sin_size = sizeof(struct sockaddr_in);  
    int epoll_fd; //创建一个epoll句柄  
    epoll_fd = epoll_create(MAX_EVENTS);  
    if(epoll_fd == -1)  
    {  
        perror("epoll_create failed");  
        exit(EXIT_FAILURE);  
    }  
    struct epoll_event ev;// epoll事件结构��? 
    struct epoll_event events[MAX_EVENTS];// 事件监听队列 
    setnonblocking(listen_sock); 
    ev.events = EPOLLIN | EPOLLET;  
    ev.data.fd = listen_sock;  
   if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,listen_sock, &ev) == -1)//register event  
    {  
        perror("epll_ctl:server_sockfd register failed");  
        exit(EXIT_FAILURE);  
    }  
    int nfds = 0;// epoll监听事件发生的个��? 
    //循环接受客户端请��?     
   while(1)  
  {  
        // 等待事件发生  
       nfds = epoll_wait(epoll_fd,events,HALF_MAX_EVENTS,-1);  
       if(nfds == -1)  
        {  
            perror("start epoll_wait failed");  
            exit(EXIT_FAILURE);  
        }  
        for(int i=0; i<nfds; ++i)  
        {  
            //signal(SIGINT,SIG_IGN);
            //signal(SIGPIPE,SIG_IGN);
            // 客户端有新的连接请求  
            if(events[i].data.fd == listen_sock)  
           {  
                // 等待客户端连接请求到��? 
                client_sock = accept(listen_sock,(struct sockaddr *)&client_addr,(socklen_t*)&sin_size);
                if(client_sock < 0)
                {     
                    perror("accept client_sock failed");     
                    exit(EXIT_FAILURE);  
                }  
                setnonblocking(client_sock);
                setsockopt(client_sock, SOL_SOCKET, SO_LINGER, &so_linger, sizeof(so_linger));
                 //向epoll注册client_sockfd监听事件  
                ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLERR | EPOLLOUT;
                ev.data.fd = client_sock;  
                if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,client_sock,&ev) == -1)  
                {  
                   perror("epoll_ctl:client_sock register failed");  
                    exit(EXIT_FAILURE);  
                } 
                
                printf("accept client: %s\n",inet_ntoa(client_addr.sin_addr));  
            }  
             //客户端有数据发送过��? 
            else    
            { 
                int len = recv(events[i].data.fd, buff, BUFFER_SIZE, MSG_NOSIGNAL); 
                if(len == 0)
                {  
                    perror("receive from client failed...\n");  
                    close(events[i].data.fd);
                }
                else if((events[i].events & EPOLLIN) && (events[i].events & EPOLLRDHUP) )
                {
                    close(events[i].data.fd);
                }
                printf("buff len = %d\n", strlen(buff));
                if((strlen(buff) > 0) && (buff[0] != '*')) 
                {
                    char *p = strchr(buff, '*'); 
                                if(p != NULL)
                                {
                                   memcpy(tembuff, p, strlen(p));
                                   memset(buff, 0, strlen(buff));
                                   memcpy(buff, tembuff, strlen(tembuff)); 
                                   memset(tembuff, 0, strlen(tembuff));
                                } 
                }            
 
                int bufflen = strlen(buff);
                int countnum = 0;
                for (int i = 0; i < bufflen; ++i)
                {
                        if(buff[i] == '*')
                        {
                            ++countnum;
                        }
                }
                if(countnum > 256)
                {
                        memset(buff, 0, sizeof(buff));
                        continue;
                }
                int num = countnum;  
                printf("The countnum is: %d\n", countnum);
                //char *temptr = strchr(buff, '*');
                //*temptr = '0';
                //char *spaceptr[1024*512] = {0};
                if(--countnum > 0)
                {
                                        
                        char *find = strtok(buff, "*");
                        //find = strtok(NULL, "*");
                        for(int i = 0; i < countnum + 1; ++i)
                        {
                            if(NULL == (spaceptr[i] = (char*)malloc(sizeof(char)*1024)))
                            {
                                printf("There is something wrong about malloc\n");
                                exit(1);
                            }
                            //char *temptr = strchr(buff, '*');
                            //char *find = strtok(buff, "*");
                            if(find)
                            {
                            
                                //spaceptr[countnum] = (char*)malloc(sizeof(char)*1024);
                                sprintf(spaceptr[i], "%s%s", "*", find);
                                printf("The bad package of  %d is: %s\n", i, spaceptr[i]);
                                if(i == countnum)
                                {
                                    break;
                                }
                            }
                            if(i == countnum-1)
                            {
                                find = strtok(NULL, "\0");
                                continue;
                                //spaceptr[--countnum] = (char*)malloc(sizeof(char)*128);
                                //sprintf(spaceptr[i], "%s%s", '*', find);
                                //break;
                            }
                            //--countnum;    
                            find = strtok(NULL, "*");
                                 
                        }
                }
                else
                {
                    spaceptr[0] = (char*)malloc(sizeof(char)*1024);
                    sprintf(spaceptr[0], "%s", buff);
                    printf("The command is : %s\n", spaceptr[0]);
                } 
                for (int i = 0; i < num; ++i)
                {
                    memset(buff, 0, BUFFER_SIZE);
                    memcpy(buff, spaceptr[i], 1024);

                    int count = strlen(buff);
                    if((count > 6) && (buff[0] == '*') && !strstr(buff, "keys"))//排除keys命令��?
                    {
                        //printf("The countnum is: %d\n", ++countnum);
                            if(buff[count-1] == 'a')
                            {
                                buff[count-1] = '\0';
                                if(processCommandFunc(buff) == -1)
                                {
                                    free(spaceptr[i]);
                                    spaceptr[i] = NULL;
                                    continue;
                                }
                                if( flag == 1)
                                {
                                    flag = 0;
                                    if(redisConnectFuc_b())
                                    {
                                        redisFree(conn_b);
                                    }
                                    else
                                    {
                                        nRet = redis_func_b_plus(buff, backbuf);
                                        if(nRet == 0)
                                            redisFree(conn_b);
                                    }
                                    memset(buff, 0, BUFFER_SIZE);
                                    memset(backbuf, 0, BUFFER_SIZE);
                                    memset(secbackbuf, 0, BUFFER_SIZE);
                                }
                                else
                                {
                                    if(redisConnectFuc_b())
                                    {
                                        redisFree(conn_b);
                                    }
                                    else
                                    {
                                        nRet = redis_func_b(buff);
                                        if(nRet == 0)
                                        redisFree(conn_b);
                                    }
                                    memset(buff, 0, BUFFER_SIZE);
                                    memset(secbackbuf, 0, BUFFER_SIZE);
                                }
                            }
                            else if(buff[count-1] == 'b' )
                            {
                                buff[count-1] = '\0';
                                processCommandFunc(buff);
                                if( flag == 1)
                                {
                                    flag = 0;
                                    if(redisConnectFuc_a())
                                    {
                                        redisFree(conn_a);
                                    }
                                    else
                                    {
                                        nRet = redis_func_a_plus(buff, backbuf);
                                        if(nRet == 0)
                                           redisFree(conn_a);
                                    }
                                    memset(buff, 0, BUFFER_SIZE);
                                    memset(backbuf, 0, BUFFER_SIZE);
                                    memset(secbackbuf, 0, BUFFER_SIZE);
                                }
                                else
                                {
                                    if(redisConnectFuc_a())
                                    {
                                        redisFree(conn_a);
                                    }
                                    else
                                    {
                                        nRet = redis_func_a(buff);
                                        if(nRet == 0)
                                            redisFree(conn_a);
                                    }
                                    memset(buff, 0, BUFFER_SIZE);
                                    memset(secbackbuf, 0, BUFFER_SIZE);
                                }
                            
                            }
                        
                    }
                    free(spaceptr[i]);
                    spaceptr[i] = NULL;
                }    
            }  
             
        }  
  } 
    close(client_sock );
    close(epoll_fd);
    return 0;     
}    
