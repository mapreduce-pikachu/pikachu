#include "stdio.h"
#include "string.h"
#include "stdlib.h"

int main()
{
    int i;
    FILE *fout;

    fout = fopen ("cp_file.sh", "w");
    for (i = 1; i < 41; i++)
    {
	fprintf (fout, "./hadoop dfs -put ~/Input/file_60_%d input-80/file_%d\n", i%2 + 1, 41+i);
    }
}
