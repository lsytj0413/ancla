#!/bin/bash
set -euo pipefail
files=$(find crates cli -name "*.rs") || exit 1

add() {
    for file in ${files}; do 
        if ! diff -q <(head -n 21 $file | cut -c4-) LICENSE >/dev/null ; then
            sed 's/^/\/\/ /' LICENSE | cat - $file > $file.tmp
            first_line=$(head -n 1 $file)
            if [[ ! -z "first_line" ]]; then
                sed -i '21a\\' ${file}.tmp
            fi
            sed -i 's/^\/\/ $/\/\//' ${file}.tmp
            mv ${file}.tmp $file

            echo "Added license to $file"
        fi
    done
}

remove() {
    for file in ${files}; do 
        if diff -q <(head -n 21 $file | cut -c4-) LICENSE >/dev/null ; then
            sed -i '1,21d' $file
            awk 'NF{f=1} f' "$file" > ${file}.tmp && mv ${file}.tmp "$file"
            echo "Removed license from $file"
        fi
    done
}

check() {
    exit_code=0
    for file in ${files}; do 
        if ! diff -q <(head -n 21 $file | cut -c4-) LICENSE >/dev/null ; then
            echo "$file does not contain valid license header"
            exit_code=1
        fi
    done
    exit $exit_code
}

case "$1" in
    add)
        add
        ;;
    remove)
        remove
        ;;
    check)
        check
        ;;
    *)
        echo "Usage: $0 {add}"
        exit 1
        ;;
esac
