/* This code is based on the fine code written by Joseph Pfeiffer for his
   fuse system tutorial. */

#include "s3fs.h"
#include "libs3_wrapper.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>

#define GET_PRIVATE_DATA ((s3context_t *) fuse_get_context()->private_data)

/*
 * For each function below, if you need to return an error,
 * read the appropriate man page for the call and see what
 * error codes make sense for the type of failure you want
 * to convey.  For example, many of the calls below return
 * -EIO (an I/O error), since there are no S3 calls yet
 * implemented.  (Note that you need to return the negative
 * value for an error code.)
 */

/*
 * Open directory
 *
 * This method should check if the open operation is permitted for
 * this directory
 */
int fs_opendir(const char *path, struct fuse_file_info *fi) 
{
	fprintf(stderr, "fs_opendir(path=\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	uint8_t *direc = NULL;
	size_t exists = s3fs_get_object((const char*)(ctx->s3bucket), (const char*)path, &direc, 0, 0);
	if ((int)exists < 0)//ensures that get returned successfuly
	{
		return -ENOENT;
	}
	else if ((int)exists == 0)//ensures that the object retrieved isn't empty (which would be really weird)
	{
		return -EIO;
	}
	s3dirent_t *direcbuff = NULL;
	direcbuff = (s3dirent_t*)direc;
	if (direcbuff[0].type == 'D')//ensures that the object retrieved is a directory and returns success if True or -EIO if False
	{
		return 0;
	}
	else
	{
		return -EIO;
	}
}


/* 
 * Get file attributes.  Similar to the stat() call
 * (and uses the same structure).  The st_dev, st_blksize,
 * and st_ino fields are ignored in the struct (and 
 * do not need to be filled in).
 */

int fs_getattr(const char *path, struct stat *statbuf) 
{
	//getattr assumes that the file/directory has been successfully opened, and therefore exists
	fprintf(stderr, "fs_getattr(path=\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: ENSURE PARENT DIRECTORY EXISTS
	char *pathcpy = strdup(path);
	char *direcname = dirname(pathcpy);
	uint8_t *metadireccpy = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), (const char*)direcname, &metadireccpy, 0, 0);
	if ((int)getsuccess < 0)//ensure get worked right...
	{
		return -ENOENT;
	}
	else if ((int)getsuccess == 0)
	{
		return -EIO;
	}
	//STEP 2: ITERATE THROUGH PARENT DIRECTORY SEARCHING FOR THE TARGET S3DIRENT_T
	s3dirent_t *metadirec = NULL;
	metadirec = (s3dirent_t*)metadireccpy;
	s3dirent_t currentdirent;
	int numdir = (int)getsuccess / sizeof(s3dirent_t);
	int i = 0;
	for (; i < numdir; i++)
	{
		currentdirent = metadirec[i];
		if (strcmp(currentdirent.name, path));
		{
			//STEP 3: CHECK THE FILE TYPE OF THE TARGET, FILE -> FILL METADATA FROM DIRENT IN PARENT/DIRECTORY -> FILL METADATA FROM DIRENT IN . OF ITSELF
			if (currentdirent.type == 'F')
			{
				statbuf->st_mode = currentdirent.st_mode;
				statbuf->st_uid = currentdirent.st_uid;
				statbuf->st_gid = currentdirent.st_gid;
				statbuf->st_size = currentdirent.st_size;
				return 0;
			}
			else if (currentdirent.type == 'D')
			{
				uint8_t *utargetdir = NULL;
				getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), (const char*)path, &utargetdir, 0, 0);
				if ((int)getsuccess < 0) //ensure get worked right...
				{
					return -ENOENT;
				}
				else if ((int)getsuccess == 0)
				{
					return -EIO;
				}
				s3dirent_t *targetdir = NULL;
				targetdir = (s3dirent_t*)utargetdir;
				statbuf->st_mode = targetdir[0].st_mode;
				statbuf->st_uid = targetdir[0].st_uid;
				statbuf->st_gid = targetdir[0].st_gid;
				statbuf->st_size = targetdir[0].st_size;
				return 0;
			}
		}
	}
	return -EIO;
}


/* 
 * File open operation
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.
 * 
 * Optionally open may also return an arbitrary filehandle in the 
 * fuse_file_info structure (fi->fh).
 * which will be passed to all file operations.
 * (In stages 1 and 2, you are advised to keep this function very,
 * very simple.)
 */
int fs_open(const char *path, struct fuse_file_info *fi)
{
	//STEP 1: ENSURE THAT THE FILE EXISTS
	fprintf(stderr, "fs_open(path\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	uint8_t *placeholder = NULL;
	size_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), (const char*)path, &placeholder, 0, 0);
	if ((int)getsuccess < 0)//ensures that the object exists
	{
		return -ENOENT;
	}
	//STEP 2: ENSURE THAT THE PARENT DIRECTORY (AND THEREFORE METADATA) EXISTS
	uint8_t *udirec = NULL;
	char *pathcpy = strdup(path);
	char *direcname = dirname(pathcpy);
	getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), (const char*)direcname, &udirec, 0, 0);
	if ((int)getsuccess < 0)//ensures that the parent exists and isn't empty (which would be really weird)
	{
		return -ENOENT;
	}
	else if ((int)getsuccess == 0)
	{
		return -EIO;
	}
	//STEP 3: ENSURE THAT THE OBJECT IS A FILE
	s3dirent_t *direc = NULL;
	direc = (s3dirent_t*)udirec;
	int numdirents = (int)getsuccess / sizeof(s3dirent_t);
	int i = 0;
	for (; i < numdirents; i++)
	{
		if (strcmp(direc[i].name, path) == 0)
		{
			if(direc[i].type == 'F')
			{
				return 0;
			}
			else
			{
				return -ENOENT;
			}
		}
	}
	return -EIO;
}


/* 
 * Create a file "node".  When a new file is created, this
 * function will get called.  
 * This is called for creation of all non-directory, non-symlink
 * nodes.  You *only* need to handle creation of regular
 * files here.  (See the man page for mknod (2).)
 */
int fs_mknod(const char *path, mode_t mode, dev_t dev)
{
	fprintf(stderr, "fs_mknod(path=\"%s\", mode=0%3o)\n", path, mode);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: ENSURE THE PARENT EXISTS AND IS A VALID DIRECTORY AND THAT THE NEW FILE DOESN'T ALREADY EXIST
	char *pathcpy = strdup(path);
	char *direcname = dirname(pathcpy);
	int opensuccess = fs_opendir((const char *)direcname, NULL);
	if (opensuccess != 0)//ensures that parent direc opened
	{
		return -ENOENT;
	}
	opensuccess = fs_open(path, NULL);
	if (opensuccess == 0)//ensures that file didn't open because it shouldn't exist
	{
		return -EEXIST;
	}
	//STEP 2: GET THE PARENT DIRECTORY INTO A BUFFER
	uint8_t *udirarr = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), (const char*)direcname, &udirarr, 0, 0);
	if ((int)getsuccess < 0)//ensures that get was successful
	{
		return -ENOENT;
	}
	else if ((int)getsuccess == 0)
	{
		return -EIO;
	}
	s3dirent_t *dirarr = NULL;
	dirarr = (s3dirent_t*)udirarr;
	//STEP 3: COPY PARENT DIRECTORY OVER TO A BUFFER WITH SPACE FOR 1 MORE DIRENT, THEN FILL NEWNODE DIRENT INTO THE LAST SPOT IN THE ARRAY
	//SIMULTANEOUSLY ENSURES THAT SOME OTHER DIRENT WITH THE SAME NAME DOESN'T ALREADY EXIST HERE (IT SHOULDN'T)
	int numdirent = (int)getsuccess / sizeof(s3dirent_t);
	s3dirent_t *newdirarr = malloc ((numdirent +1)*(sizeof (s3dirent_t)));
	int i = 0;
	for (; i < numdirent ; i++)
	{
		if (strcmp(dirarr[i].name,path) == 0) //ensures that the parent doesn't already hold an entry for this file. This is a recoverable error, but it would only happen if something very weird happened previously and the user should get an error so that they know
		{
			return -EEXIST;
		}
		newdirarr[i] = dirarr[i];
	}
	s3dirent_t metadata;
	//STEP 4: FILL METADATA INTO A NEW DIRENT AND THEN STORE THE NEWLY UPDATED PARENT DIRECTORY IN S3
	uid_t usr = getuid();
	gid_t group = getgid();
	metadata.type = 'F';
	strncpy(metadata.name, path, 256);
	metadata.st_uid = usr;
	metadata.st_gid = group;
	metadata.st_mode = mode;
	metadata.st_size = 0;
	newdirarr[numdirent] = metadata;
	newdirarr[0].st_size = newdirarr[0].st_size + sizeof(s3dirent_t); //change size of parent
	ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), (const char*)direcname, (uint8_t*)newdirarr, newdirarr[0].st_size);
	if ((int)putsuccess <= 0)//ensures that put was successful
	{
		return -EIO;
	}
	//STEP 5: STORE THE NEW FILE (WHICH IS JUST NULL) IN S3
	putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), path, NULL, 0);
	if ((int)putsuccess <= 0)//ensures that put was successful
	{
		return -EIO;
	}
	return 0;
}


/* 
 * Create a new directory.
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits (for setting in the metadata)
 * use mode|S_IFDIR.
 */
int fs_mkdir(const char *path, mode_t mode)
{
	fprintf(stderr, "fs_mkdir(path=\"%s\", mode=0%3o)\n", path, mode);
	s3context_t *ctx = GET_PRIVATE_DATA;
	mode |= S_IFDIR;
	//STEP 1: ENSURE THAT THE DIRECTORY TO BE CREATED DOESN'T ALREADY EXIST
	int opensuccess = fs_opendir(path, NULL);
	if (opensuccess == 0)
	{
		return -EEXIST;
	}
	//STEP 2: ENSURE THAT THE DIRECTORY'S PARENT EXISTS
	char *pathcpy = malloc(sizeof(path));
	strcpy(pathcpy, path);
	char *direcname = dirname(pathcpy); //uses a copy because dirname command alters input string
	uint8_t *upardir = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), direcname, &upardir, 0, 0);
	if ((int)getsuccess < 0)
	{
		return -ENOENT;
	}
	else if ((int)getsuccess == 0)
	{
		return -EIO;
	}
	s3dirent_t *pardir = NULL;
	pardir = (s3dirent_t*)upardir;
	//STEP 3: ITERATE THROUGH PARENT DIRECTORY COPYING OVER DATA TO A 1 DIRENT LARGER BUFFER TO BE STORED AS THE NEW ARRAY WITH THE NEW DIR'S DIRENT INCLUDED
	int numdirent = (int)getsuccess / sizeof(s3dirent_t);
	s3dirent_t *tempdir = malloc(sizeof(s3dirent_t) * (numdirent + 1));
	int i = 0;
	for (; i < numdirent; i++)
	{
		tempdir[i] = pardir[i];
	}
	//STEP 4: FLL METADATA INTO A DIRENT AND PUT IT INTO THE NEW PARENT ARRAY
	s3dirent_t newself;
	strncpy(newself.name, path, 256);
	newself.type = 'D';
	newself.st_uid = 0;
	newself.st_gid = 0;
	newself.st_mode = 0;
	newself.st_size = 0;
	tempdir[numdirent] = newself;
	tempdir[0].st_size = tempdir[0].st_size + sizeof(s3dirent_t);
	//STEP 5: CREATE NEWDIR AND IT'S METADATA DIRENT AND PLACE THE DIRENT AT NEWDIR[0]
	s3dirent_t* newdir = malloc(sizeof(s3dirent_t));
	uid_t user = getuid();
	gid_t group = getgid();
	newself.name[0] = '.';
	newself.name[1] = '\0';
	newself.type = 'D';
	newself.st_uid = user;
	newself.st_gid = group;
	newself.st_mode = mode;
	newself.st_size = sizeof(s3dirent_t);
	newdir[0] = newself;
	//STEP 6: PUT NEW PARENT DIRECTORY AND NEW CHILD DIRECTORY INTO S3
	tempdir[0].st_size = tempdir[0].st_size + sizeof(s3dirent_t); //update parent directory size
	ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), direcname, (uint8_t*)tempdir, (sizeof(s3dirent_t) * (numdirent + 1)));
	if ((int)putsuccess <= 0)
	{
		return -EIO;
	}
	putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), path, (uint8_t*)newdir, sizeof(s3dirent_t));
	if ((int)putsuccess < 0)
	{
		return -EIO;
	}
	return 0;
}

/*
 * Remove a file.
 */
int fs_unlink(const char *path)
{
	fprintf(stderr, "fs_unlink(path=\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: ENSURE THAT THE FILE TO BE UNLINKED EXISTS, AND RETRIEVE IT'S PARENT DIRECTORY
	int opensuccess = fs_open(path, NULL);
	if (opensuccess == 0)
	{
		return -EEXIST;
	}
	char *pathcpy = strdup(path);
	char *direcname = dirname(pathcpy);
	uint8_t *upardir = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), direcname, &upardir, 0, 0);
	if ((int)getsuccess < 0)
	{
		return -ENOENT;
	}
	else if ((int)getsuccess == 0)
	{
		return -EIO;
	}
	s3dirent_t *pardir = NULL;
	pardir = (s3dirent_t *)upardir;
	//STEP 2: ITERATE THROUGH PARENT DIRECTORY, COPYING OVER TO A NEW 1 SMALLER ARRAY, AND NOT INCLUDING THE DIRENT OF THE FLE TO BE REMOVED
	int numdirents = (int)getsuccess / sizeof(s3dirent_t); 
	int i = 0;
	int j = 0;
	s3dirent_t *newpardir = malloc(sizeof(s3dirent_t) * (numdirents - 1));
	for (; i < numdirents ; i++)
	{
		if(strcmp(pardir[i].name,path) != 0)
		{
			newpardir[i] = pardir[i];
			j++;
		}
	}
	if (j == (i - 1))
	{
		//STEP 3: PUT THE ALTERED PARENT DIRECTORY AND REMOVE THE FILE FROM S3
		newpardir[0].st_size = newpardir[0].st_size - sizeof(s3dirent_t); //update size of parent directory
		ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), direcname, (uint8_t*)newpardir, newpardir[0].st_size);
		if ((int)putsuccess <= 0)
		{
			return -EIO;
		}
		int rmsuccess = s3fs_remove_object((const char*)(ctx->s3bucket), path);
		if (rmsuccess < 0)
		{
			return -EIO;
		}
		return 0;
	}
	return -EIO;
}

/*
 * Remove a directory. 
 */
int fs_rmdir(const char *path)
{
	fprintf(stderr, "fs_rmdir(path=\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: ENSURE THAT DIR TO BE REMOVED EXISTS AND HAS ONLY IT'S SELF ENTRY
	uint8_t *udirbuf = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), path, &udirbuf, 0, 0);
	if ((int)getsuccess < 0)
	{
		return -ENOENT;
	}
	else if ((int)getsuccess == 0)
	{
		return -EIO;
	}
	s3dirent_t *dirbuf = NULL;
	dirbuf = (s3dirent_t*)udirbuf;
	int numdir = (int)getsuccess/sizeof(s3dirent_t);
	if (numdir > 1)
	{
		return -ENOTEMPTY;
	}
	else if (strcmp(".", dirbuf[0].name) != 0)
	{
		return -EIO;
	}
	//STEP 2: ENSURE THAT PARENT DIR EXISTS AND RETRIEVE IT
	char *pathcpy = malloc(sizeof(path));
	strcpy(pathcpy, path);
	char *direcname = dirname(pathcpy); //uses a copy because dirname command alters input string
	udirbuf = NULL;
	getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), direcname, &udirbuf, 0, 0);
	if ((int)getsuccess < 0)
	{
		return -ENOENT;
	}
	else if ((int)getsuccess == 0)
	{
		return -EIO;
	}
	dirbuf = NULL; //holds parent directory stuff while we make a bigger directory and transfer it over
	dirbuf = (s3dirent_t*)udirbuf;
	//STEP 3: ITERATE THROUGH PARENT DIR TO COPY OVER ALL BUT THE DIRENT TO BE REMOVED
	numdir = (int)getsuccess/sizeof(s3dirent_t);
	s3dirent_t *newdir = malloc(sizeof(s3dirent_t) * (numdir - 1));
	int i = 0;
	int j = 0;
	for (; i < numdir; i++)
	{
		if (strcmp(dirbuf[i].name, path) != 0)
		{
			newdir[j] = dirbuf[i];
			j++;
		}
	}
	if (j == (i - 1))
	{
		//STEP 4: PUT THE NEW PARENT AND REMOVE THE DIR
		newdir[0].st_size = sizeof(s3dirent_t) * (numdir - 1); //change parent size
		ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), path, (uint8_t*)newdir, sizeof(s3dirent_t) * (numdir - 1));
		if ((int)putsuccess <= 0)
		{
			return -EIO;
		}
		int rmsuccess = s3fs_remove_object((const char*)(ctx->s3bucket), path);
		if (rmsuccess < 0)
		{
			return -EIO;
		}
		return 0;
	}
	return -EIO;
}

/*
 * Rename a file.
 */
int fs_rename(const char *path, const char *newpath)
{
	fprintf(stderr, "fs_rename(fpath=\"%s\", newpath=\"%s\")\n", path, newpath);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: ENSURE THAT THE FILE TO BE RENAMED EXISTS AND THAT THE NEW FILENAME ISN'T TAKEN AS EITHER A FILE OR A DIRECTORY
	int opensuccess = fs_open(path, NULL);
	if (opensuccess != 0)
	{
		return -ENOENT;
	}
	opensuccess = fs_open(newpath, NULL); //this only works if fs_open only returns error if the file doesn't exist
	int opensuccess2 = fs_opendir(newpath, NULL);
	if (opensuccess == 0 || opensuccess2 == 0)
	{
		return -EIO;
	}
	//STEP 2: GET THE CURRENT PARENT DIRECTORY
	char *pathcpy = strdup(path);
	char *newpathcpy = strdup(newpath);
	char *direcname = dirname(pathcpy);
	char *newdirecname = dirname(newpathcpy);
	uint8_t *upathparent = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), direcname, &upathparent, 0, 0);
	if ((int)getsuccess <= 0)
	{
		return -ENOENT;
	}
	s3dirent_t *pathparent = NULL; //holds parent directory stuff while we make a bigger directory and transfer it over
	pathparent = (s3dirent_t*)upathparent;
	//STEP 3: FIND THE DIRENT IN THE CURRENT PARENT
	int numdirents = (int)getsuccess/sizeof(s3dirent_t);
	int i = 0;
	int fileindex = -1;
	for (;i<numdirents;i++)
	{
		if (strcmp(pathparent[i].name,path) == 0)
		{
			fileindex = i;
			i = numdirents;
		}
	}
	if (fileindex <= 0)
	{
		return -EIO;
	}
	//STEP 4A: IF THE DIRECTORY OF THE NEW AND CURRENT ARE THE SAME, SIMPLY RENAME THE DIRENT AND STORE THE FILE IN THE NEW KEY
	if (strcmp(direcname, newdirecname) == 0)
	{
		strncpy(pathparent[fileindex].name, newpath, 256);
		uint8_t *ufcontents = NULL;
		getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), path, &ufcontents, 0, 0); //get the file contents from s3
		if ((int)getsuccess < 0)
		{
			return -ENOENT;
		}
		ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), direcname, (uint8_t*)pathparent, (sizeof(s3dirent_t) * numdirents)); //put the parent dir with updated dirent in s3
		if ((int)putsuccess <= 0)
		{
			return -EIO;
		}
		putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), newpath, ufcontents, getsuccess); //put the file contents in the new file location
		if ((int)putsuccess < 0)
		{
			return -EIO;
		}
		int rmsuccess = s3fs_remove_object((const char*)(ctx->s3bucket), path); //remove the old file
		if (rmsuccess < 0)
		{
			return -EIO;
		}
		return 0;
	}
	//STEP 4B: OTHERWISE ITERATE THROUGH NEW PARENT, COPYING OVER TO A +1 SIZED BUFFER, THEN PUT THE NEW BUFFER WITH NEW DIRENT IN S3
	else
	{
		uint8_t *unewparent = NULL;
		getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), newdirecname, &unewparent, 0, 0); //get the contents of the new directory
		if ((int)getsuccess <= 0)
		{
			return -ENOENT;
		}
		s3dirent_t *newparent = NULL;
		newparent = (s3dirent_t*)unewparent;
		int newnumdirents = (int)getsuccess / sizeof(s3dirent_t);
		//STEP 5: FIX THE NEW DIRECTORY'S SIZE AND CONTENTS TO REFLECT THE FILE YOU'VE ADDED
		s3dirent_t *newnewparent = malloc(sizeof(s3dirent_t) * (newnumdirents + 1)); //allocate new array for new directory
		int j = 0;
		for (;j<newnumdirents;j++) //copy over new directory
		{
			newnewparent[j] = newparent[j];
		}
		newnewparent[newnumdirents] = pathparent[fileindex]; //add new dirent
		newnewparent[0].st_size = newnewparent[0].st_size + sizeof(s3dirent_t); //update new parent size data
		ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), newdirecname, (uint8_t*)newnewparent, ((newnumdirents + 1) * (sizeof(s3dirent_t)))); //put the new parent dirent in s3
		if ((int)putsuccess <= 0)
		{
			return -EIO;
		}
		uint8_t *ufcontents = NULL;
		getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), path, &ufcontents, 0, 0); //get file contents
		if ((int)getsuccess < 0)
		{
			return -ENOENT;
		}
		putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), newpath, ufcontents, getsuccess); //put file contents in new key
		if ((int)putsuccess < 0)
		{
			return -EIO;
		}
		//STEP 6: FIX THE OLD DIRECTORY'S SIZE AND CONTENTS TO REFLECT THE FILE YOU'VE REMOVED
		s3dirent_t *newpathparent = malloc(sizeof(s3dirent_t) * (numdirents - 1)); //allocate new array for old directory
		j = 0;
		int k = 0;
		for (;j<numdirents;j++) //copy over old directory
		{
			if (strcmp(path, pathparent[j].name) != 0); //removes dirent
			{
				newpathparent[k] = pathparent[j];
				k++;
			}
		}
		newpathparent[0].st_size = newpathparent[0].st_size - sizeof(s3dirent_t); //update old parent size
		putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), direcname, (uint8_t*)pathparent, (sizeof(s3dirent_t) * newnumdirents)); //put the parent dir with removed dirent in s3
		if ((int)putsuccess <= 0)
		{
			return -EIO;
		}
		int rmsuccess = s3fs_remove_object((const char*)(ctx->s3bucket), path); //remove the old file
		if (rmsuccess < 0)
		{
			return -EIO;
		}
		return 0;
	}
	return -EIO;
}

/*
 * Change the permission bits of a file.
 */
int fs_chmod(const char *path, mode_t mode)
{
    fprintf(stderr, "fs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Change the owner and group of a file.
 */
int fs_chown(const char *path, uid_t uid, gid_t gid)
{
    fprintf(stderr, "fs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Change the size of a file.
 */
int fs_truncate(const char *path, off_t newsize)
{
	fprintf(stderr, "fs_truncate(path=\"%s\", newsize=%d)\n", path, (int)newsize);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: FIND METADATA IN PARENT AND CHANGE SIZE TO 0
	char *pathcpy = strdup(path);
	char *direcname = dirname(pathcpy);
	uint8_t *upardir = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), direcname, &upardir, 0, 0);
	if ((int)getsuccess <= 0)
	{
		return -ENOENT;
	}
	s3dirent_t *pardir = NULL;
	pardir = (s3dirent_t *)upardir;
	int numdirents = (int)getsuccess / sizeof(s3dirent_t);
	int i = 0;
	for (;i<numdirents;i++)
	{
		if(strcmp(pardir[i].name,path) == 0)
		{
			pardir[i].st_size = 0;
		}
	}
	//STEP 2: PUT FIXED PARENT AND 0-LENGTH FILE IN S3
	ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), direcname, (uint8_t*)pardir, getsuccess); //put parent directory with updated dirent in s3
	if ((int)putsuccess <= 0)
	{
		return -EIO;
	}
	putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), path, NULL, 0); //change file to a zero length NULL
	if ((int)putsuccess <= 0)
	{
		return -EIO;
	}
	return 0;
}

/*
 * Change the access and/or modification times of a file. 
 */
int fs_utime(const char *path, struct utimbuf *ubuf)
{
    fprintf(stderr, "fs_utime(path=\"%s\")\n", path);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}




/* 
 * Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  
 */
int fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	fprintf(stderr, "fs_read(path=\"%s\", buf=%p, size=%d, offset=%d)\n", path, buf, (int)size, (int)offset);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: GET THE FILE
	//Do we need to get metadata and make sure that readsize is <= size of the file??
	uint8_t *ubuf = NULL;
	size_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), path, &ubuf, (ssize_t)offset, (ssize_t)size);
	if ((int)getsuccess < 0)
	{
		return -EIO;
	}
	//STEP 2: COPY THE FILE INTO THE GIVEN BUFFER
	else if((int)getsuccess == (int)size || (((int)size == 0) && ((int)offset == 0)))//Does the number of bytes written have to be == to the number of bytes requested???
	{	
		buf = (char*)ubuf;
		return getsuccess;
	}	
    return -EIO;
}

/*
 * Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.
 */
int fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	fprintf(stderr, "fs_write(path=\"%s\", buf=%p, size=%d, offset=%d)\n", path, buf, (int)size, (int)offset);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: READ IN THE CONTENTS OF THE FILE TO WRITE TO
	char *readbuf = NULL;
	int readsuccess = fs_read(path, readbuf, 0, 0, NULL);
	if (readsuccess != 0)
	{
		return -ENOENT;
	}
	int bufsize = strlen(readbuf);
	if ((int)offset > bufsize)
	{
		return -EIO;
	}
	//STEP 2: DETERMINE THE SIZE THAT THE NEW FILE WILL BE
	int newsize = 0;
	if (bufsize >= ((int)offset + (int)size))
	{
		newsize = bufsize;
	}
	else
	{
		newsize = (int)offset + (int)size;
	}
	char *newbuff = malloc(sizeof(char) * (newsize));
	//STEP 3: COPY THE OLD CONTENTS AND NEW INPUT INTO THE NEW FILE
	int i = (int)offset;
	int j = 0;
	for (; i < newsize; i++)
	{
		if ((i >= (int)offset) && (i < (int)size)) //overwrite with new input
		{
			newbuff[i] = buf[j];
			j++;
		}
		else //copy from the original file
		{
			newbuff[i] = readbuf[i];
		}
	}
	//STEP 4: CHANGE THE SIZE IN METADATA IF NECESSARY
	if (newsize > bufsize)
	{
		char *pathcpy = strdup(path);
		char *direcname = dirname(pathcpy);
		uint8_t *upardir = NULL;
		ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), direcname, &upardir, 0, 0);
		if ((int)getsuccess <= 0)
		{
			return -ENOENT;
		}
		s3dirent_t *pardir = NULL;
		pardir = (s3dirent_t *)upardir;
		int numdirents = (int)getsuccess / sizeof(s3dirent_t);
		int i = 0;
		for (; i < numdirents ; i++)
		{
			if(strcmp(pardir[i].name, path) == 0)
			{
				pardir[i].st_size = newsize;
			}
		}
	}
	//STEP 5: PUT THE NEW FILE INTO S3 AND ALTER THE METADATA OF THE FILE TO FIT THE NEW SIZE
	ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), path, (uint8_t*)readbuf, sizeof(readbuf));
	if ((int)putsuccess < 0)
	{
		return -EIO;
	}
    	return -EIO;
}


/* 
 * Possibly flush cached data for one file.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 */
int fs_flush(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "fs_flush(path=\"%s\", fi=%p)\n", path, fi);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return -EIO;
}

/*
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.  
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 */
int fs_release(const char *path, struct fuse_file_info *fi)
{
	fprintf(stderr, "fs_release(path=\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	return 0;
}

/*
 * Synchronize file contents; any cached data should be written back to 
 * stable storage.
 */
int fs_fsync(const char *path, int datasync, struct fuse_file_info *fi) 
{
	fprintf(stderr, "fs_fsync(path=\"%s\")\n", path);
	return -EIO;
}

/*
 * Read directory.  See the project description for how to use the filler
 * function for filling in directory items.
 */
int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	fprintf(stderr, "fs_readdir(path=\"%s\", buf=%p, offset=%d)\n", path, buf, (int)offset);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: GET THE CONTENTS OF THE DIRECTORY FROM S3
	uint8_t *udirec = NULL;
	size_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), path, &udirec, 0, 0);
	if (getsuccess <= 0)
	{
		return -EIO;
	}
	s3dirent_t *direc = NULL;
	direc = (s3dirent_t*)udirec;
	//STEP 2: ITERATE THROUGH THE DIRECTORY AND USE FILLER TO FILL THE GIVEN BUFFER WITH THE CONTENTS
	int numdirent = getsuccess / sizeof(s3dirent_t);
	int i = 0;
	for (; i < numdirent; i++) 
	{
		// call filler function to fill in directory name
		// to the supplied buffer
		if (filler(buf, direc[i].name, NULL, 0) != 0) 
		{
			return -ENOMEM;
		}
	}
	return 0;
}

/*
 * Release directory.
 */
int fs_releasedir(const char *path, struct fuse_file_info *fi) 
{
	fprintf(stderr, "fs_releasedir(path=\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	return 1;
}

/*
 * Synchronize directory contents; cached data should be saved to 
 * stable storage.
 */
int fs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi) 
{
	fprintf(stderr, "fs_fsyncdir(path=\"%s\")\n", path);
	s3context_t *ctx = GET_PRIVATE_DATA;
	return -EIO;
}

/*
 * Initialize the file system.  This is called once upon
 * file system startup.
 */
void *fs_init(struct fuse_conn_info *conn)
{
	fprintf(stderr, "fs_init --- initializing file system.\n");
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: CLEAR THE BUCKET
	s3fs_clear_bucket((const char*)(ctx->s3bucket));
	//STEP 2: CREATE A ROOT DIRECTORY AND FILL IT WITH IT'S SELF DIREC
	s3dirent_t *root = malloc(sizeof(s3dirent_t));
	s3dirent_t rself;
	gid_t group = getgid();
	uid_t usr = getuid();
	mode_t mode = (S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR);
	rself.name[0] = '.';
	rself.name[1] = '\0';
	rself.type = 'D';
	rself.st_uid = usr;
	rself.st_gid = group;
	rself.st_mode = mode;
	rself.st_size = sizeof(s3dirent_t);
	root[0] = rself;
	//STEP 3: PUT THE ROOT DIRECTORY INTO S3
	s3fs_put_object((const char*)(ctx->s3bucket), "/", (uint8_t*)root, sizeof(s3dirent_t)); 
	return ctx;
}

/*
 * Clean up filesystem -- free any allocated data.
 * Called once on filesystem exit.
 */
void fs_destroy(void *userdata)
{
	fprintf(stderr, "fs_destroy --- shutting down file system.\n");
	s3context_t *ctx = GET_PRIVATE_DATA;
	s3fs_clear_bucket((const char*)(ctx->s3bucket));
    	free(userdata);
}

/*
 * Check file access permissions.  For now, just return 0 (success!)
 * Later, actually check permissions (don't bother initially).
 */
int fs_access(const char *path, int mask) 
{
    fprintf(stderr, "fs_access(path=\"%s\", mask=0%o)\n", path, mask);
    s3context_t *ctx = GET_PRIVATE_DATA;
    return 0;
}

/*
 * Change the size of an open file.  Very similar to fs_truncate (and,
 * depending on your implementation), you could possibly treat it the
 * same as fs_truncate.
 */
int fs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi) 
{
	fprintf(stderr, "fs_ftruncate(path=\"%s\", offset=%d)\n", path, (int)offset);
	s3context_t *ctx = GET_PRIVATE_DATA;
	//STEP 1: FIND METADATA IN PARENT AND CHANGE SIZE TO 0
	char *pathcpy = strdup(path);
	char *direcname = dirname(pathcpy);
	uint8_t *upardir = NULL;
	ssize_t getsuccess = s3fs_get_object((const char*)(ctx->s3bucket), direcname, &upardir, 0, 0);
	if ((int)getsuccess <= 0)
	{
		return -ENOENT;
	}
	s3dirent_t *pardir = (s3dirent_t *)upardir;
	int numdirents = (int)getsuccess / sizeof(s3dirent_t);
	int i = 0;
	for (;i<numdirents;i++)
	{
		if(strcmp(pardir[i].name,path) == 0)
		{
			pardir[i].st_size = 0;
		}
	}
	//STEP 2: PUT FIXED PARENT AND 0-LENGTH FILE IN S3
	ssize_t putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), direcname, (uint8_t*)pardir, getsuccess); //put parent directory with updated dirent in s3
	if ((int)putsuccess <= 0)
	{
		return -EIO;
	}
	putsuccess = s3fs_put_object((const char*)(ctx->s3bucket), path, NULL, 0); //change file to a zero length NULL
	if ((int)putsuccess <= 0)
	{
		return -EIO;
	}
	return 0;
}

/*
 * The struct that contains pointers to all our callback
 * functions.  Those that are currently NULL aren't 
 * intended to be implemented in this project.
 */
struct fuse_operations s3fs_ops = {
  .getattr     = fs_getattr,    // get file attributes
  .readlink    = NULL,          // read a symbolic link
  .getdir      = NULL,          // deprecated function
  .mknod       = fs_mknod,      // create a file
  .mkdir       = fs_mkdir,      // create a directory
  .unlink      = fs_unlink,     // remove/unlink a file
  .rmdir       = fs_rmdir,      // remove a directory
  .symlink     = NULL,          // create a symbolic link
  .rename      = fs_rename,     // rename a file
  .link        = NULL,          // we don't support hard links
  .chmod       = fs_chmod,      // change mode bits
  .chown       = fs_chown,      // change ownership
  .truncate    = fs_truncate,   // truncate a file's size
  .utime       = fs_utime,      // update stat times for a file
  .open        = fs_open,       // open a file
  .read        = fs_read,       // read contents from an open file
  .write       = fs_write,      // write contents to an open file
  .statfs      = NULL,          // file sys stat: not implemented
  .flush       = fs_flush,      // flush file to stable storage
  .release     = fs_release,    // release/close file
  .fsync       = fs_fsync,      // sync file to disk
  .setxattr    = NULL,          // not implemented
  .getxattr    = NULL,          // not implemented
  .listxattr   = NULL,          // not implemented
  .removexattr = NULL,          // not implemented
  .opendir     = fs_opendir,    // open directory entry
  .readdir     = fs_readdir,    // read directory entry
  .releasedir  = fs_releasedir, // release/close directory
  .fsyncdir    = fs_fsyncdir,   // sync dirent to disk
  .init        = fs_init,       // initialize filesystem
  .destroy     = fs_destroy,    // cleanup/destroy filesystem
  .access      = fs_access,     // check access permissions for a file
  .create      = NULL,          // not implemented
  .ftruncate   = fs_ftruncate,  // truncate the file
  .fgetattr    = NULL           // not implemented
};



/* 
 * You shouldn't need to change anything here.  If you need to
 * add more items to the filesystem context object (which currently
 * only has the S3 bucket name), you might want to initialize that
 * here (but you could also reasonably do that in fs_init).
 */
int main(int argc, char *argv[]) {
    // don't allow anything to continue if we're running as root.  bad stuff.
    if ((getuid() == 0) || (geteuid() == 0)) {
    	fprintf(stderr, "Don't run this as root.\n");
    	return -1;
    }
    s3context_t *stateinfo = malloc(sizeof(s3context_t));
    memset(stateinfo, 0, sizeof(s3context_t));

    char *s3key = getenv(S3ACCESSKEY);
    if (!s3key) {
        fprintf(stderr, "%s environment variable must be defined\n", S3ACCESSKEY);
    }
    char *s3secret = getenv(S3SECRETKEY);
    if (!s3secret) {
        fprintf(stderr, "%s environment variable must be defined\n", S3SECRETKEY);
    }
    char *s3bucket = getenv(S3BUCKET);
    if (!s3bucket) {
        fprintf(stderr, "%s environment variable must be defined\n", S3BUCKET);
    }
    strncpy((*stateinfo).s3bucket, s3bucket, BUFFERSIZE);

    fprintf(stderr, "Initializing s3 credentials\n");
    s3fs_init_credentials(s3key, s3secret);

    fprintf(stderr, "Totally clearing s3 bucket\n");
    s3fs_clear_bucket(s3bucket);

    fprintf(stderr, "Starting up FUSE file system.\n");
    int fuse_stat = fuse_main(argc, argv, &s3fs_ops, stateinfo);
    fprintf(stderr, "Startup function (fuse_main) returned %d\n", fuse_stat);
    
    return fuse_stat;
}
