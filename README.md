# redisqueue

## Redis stream 명령어

- XREADGROUP : 새메시지 가져오기, 소유권 획득
- XPENDING : 소유권있지만 ACK안한 메세지 조회
- XCLAIM : 남이 소유한 메시지 뺐기, 소유권 이전
- XACK : 처리 완료, 소유권 해제
- XAUTOCLAIM : (XPENDING + XCLAIM)

## Redis heartbeat

- 내 메시지를 다시 XCLAIM하면 idle_time이 0으로 초기화됨
- 일정 시간이 지난 작업에 대해서 큐로 자동으로 넘기진 못하고 idle_time이 오래된걸 조회해서 소유권 이전후 따로 처리해야함 (XPENDING + XCLAIM)

## REDIS Stream 용어

- Stream : 큐, 메시지가 저장되는 곳
- MesageID : 각 메시지 고유 ID
- ConsumerGroup : 메시지를 나눠처리하는팀
- Consumer : Group 내 개별 Worker

\* ConsumerGroup이 하나의 Stream을 바라보면 하나의 메세지를 여러 그룹에서 받아서 처리가능 (알림 + DB저장)

## SSE 와 Websocket

- SSE는 브라우저 자체 자동연결 지원해줌, 대신 단방향(서버->Front)
- Websocket은 자동연결 없음, 양방향
