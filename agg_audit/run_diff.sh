#!/bin/zsh
# 差分测试运行器: 在 mydb(4837) 和 MySQL(3306) 上执行同一组查询并比较结果
# 查询文件每行一条查询; 以 "#" 开头的行是注释
# 输出: 原始顺序比较(检测行序/合计行位置) + 排序后比较(检测内容)
MYDB="mysql -h127.0.0.1 -P4838 -uroot -proot --batch --raw -D agg_audit"
MYSQL="mysql -h127.0.0.1 -P3306 -uroot --batch --raw -D agg_audit"
QF="$1"
PASS=0; FAIL=0; idx=0
while IFS= read -r line; do
  [[ -z "$line" || "$line" == \#* ]] && continue
  idx=$((idx+1))
  m_out=$(${=MYDB} -e "$line" 2>&1 | grep -v "Using a password")
  s_out=$(${=MYSQL} -e "$line" 2>&1 | grep -v "Using a password")
  if [[ "$m_out" == "$s_out" ]]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    echo "=========== DIFF #$idx ==========="
    echo "SQL: $line"
    echo "--- mydb ---"; print -r -- "$m_out"
    echo "--- mysql ---"; print -r -- "$s_out"
    echo
  fi
done < "$QF"
echo "RESULT: PASS=$PASS FAIL=$FAIL"
