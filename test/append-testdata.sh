#!/bin/bash

# append-testdata.sh source_dir target_dir variation_id
# always init target_dir if it does not exist or update it if it does exist
# if the target_dir does not exist it will be created (if it exists as a file an exception will be thrown)
# if the target_dir is given as a relative path it is always relative to the source_dir
# if the source_dir is given as a relative path it is always relative to the directory where this script is run.

# WARNING: Always use the same source_dir and target_dir pair. Since it is possible
#          merge additional variation_id related records into an existing target_dir
#          it is vital that the same source_dir be used. Otherwise data errors can
#          and will likely occur.

# if the correct args are provided then attempt to initialize environment
errmsg=
if [[ ( $# -eq 3 ) && ( -d $1 ) ]]; then
    if ! [[ $3 =~ ^[0-9]+$ ]]; then
        errmsg="Variation ID (3rd argument) is not a valid number."
    else
        cd $1
        if [[ ( ! -e $2 ) ]]; then
            mkdir $2
        elif [[ ( ! -d $2 ) ]]; then
            cd -
            errmsg="Target directory name is not an available or valid directory name."
        fi
    fi
else
    errmsg="Wrong number of arguments or source directory does not exist or is not a directory."
fi

# if we are not located in the source directory and can verify the target directory exists then throw exception
if [[ ( ! -z "$errmsg" ) ]]; then
    echo $errmsg
    echo '   Usage... loop.sh <source_dir> <target_dir> <variation_id>'
    echo
    exit 1
fi

source_dir="$1"
target_dir="$2"
variation_id="$3"

# NOTE: above we cd to the source_dir so the rysnce commands will work from the current directory '.'
# this command updates the directory structure in the target directory to mirror the source directory without any copying any files
rsync -rRu  --include={'created/','updated/','deleted/'} --include={'var*/','rcv*/','clinical*/','gene*/','trait*/','subm*/'} --include '20*/' --exclude '*' . $target_dir

record_log="${target_dir}/record.log"
if [[ (! -f $record_log ) ]]; then
    touch $record_log
    echo '${record_log} created.'
fi

# this loops through all the 0000* files in the source directory and creates an empty parallel file in the target directory
# if it does not already exist
find 20*/ -name '0000*' | while read filename; do
    out_file="${target_dir}/${filename}"
    if [ ! -f $out_file ]; then
        touch $out_file
        echo $filename
    fi
done;

# update all release_date.txt, gene, trait, trait_set, submission and submitter files to assure they are in sync with the baseline set.
#rsync -rmRd  --include={'0000*','release_date.txt'} --include={'created/','updated/','deleted/'} --include={'gene/','trait/','trait_set/','subm*/'} --include '20*/' --exclude '*' . $target_dir
rsync -rmRu  --include={'0000*','release_date.txt'} --include={'created/','updated/','deleted/'} --include={'gene/','trait/','trait_set/','subm*/'} --include '20*/' --exclude '*' . $target_dir

# create a tmp folder within the target_dir to keep working files used below, presumes we are still working in the source_dir
tmpdir="${target_dir}/tmp"
if [[ ( -e $tmpdir )]]; then
    echo "Warning: ${tmpdir} exists and is being removed automatically."
    rm -fR $tmpdir
fi
mkdir $tmpdir

# read file with list of variation ids to be loaded and load them.
# e.g. 12610, 3520, 37594, 46503, 48176, 7884
primary_var_id=$variation_id
pcre2grep -n -e "\"id\"\:\"$primary_var_id\"" -r 20*/variation/ *.json > $tmpdir/variation_records.txt
echo $primary_var_id > $tmpdir/variant_ids.txt
pcre2grep -o1 -e "\"descendant\_ids\"\:\[\s*(\".*\")\s*\]" $tmpdir/variation_records.txt >> $tmpdir/variant_ids.txt
variant_list=$( pcre2grep -o1 -e "(\d+)" $tmpdir/variant_ids.txt | sort | uniq | sed -e :a -e '$!N; s/\n/|/; ta' | awk '{print "("$0")"}' )

# build *.out files to collect all records from all *.json files in source_directory
# variation & gene_association records related to any of the $variant_list variation ids.
pcre2grep -n -e "\"id\"\:\"$variant_list\"" -r 20*/variation/ *.json > $tmpdir/variation.out
pcre2grep -n -e "\"variation_id\"\:\"$variant_list\"" -r 20*/gene_association/ *.json > $tmpdir/gene_association.out
# vcv, rcv and scv records related ONLY to the primary $variation_id.
pcre2grep -n -e "\"variation_id\"\:\"$primary_var_id\"" -r 20*/variation_archive/ *.json > $tmpdir/variation_archive.out
pcre2grep -n -e "\"variation_id\"\:\"$primary_var_id\"" -r 20*/rcv_accession/ *.json > $tmpdir/rcv_accession.out
pcre2grep -n -e "\"variation_id\"\:\"$primary_var_id\"" -r 20*/clinical_assertion/ *.json  > $tmpdir/clinical_assertion.out

# extract all scvids over time from the clinical_assertion results above to build an appropriate regex filter for the subsequent statements.
scv_list=$( pcre2grep -o1 -e "\"id\"\:\"(SCV[0-9]*)\"" $tmpdir/clinical_assertion.out | sort | uniq | sed -e :a -e '$!N; s/\n/|/; ta' | awk '{print "("$0")"}' )

# get all related SCV records
pcre2grep -n -e "$scv_list" -r 20*/clinical_assertion_trait_set/ *.json > $tmpdir/clinical_assertion_trait_set.out
pcre2grep -n -e "$scv_list" -r 20*/clinical_assertion_trait/ *.json > $tmpdir/clinical_assertion_trait.out
pcre2grep -n -e "$scv_list" -r 20*/clinical_assertion_variation/ *.json > $tmpdir/clinical_assertion_variation.out
pcre2grep -n -e "$scv_list" -r 20*/clinical_assertion_observation/ *.json > $tmpdir/clinical_assertion_observation.out
pcre2grep -n -e "$scv_list" -r 20*/trait_mapping/ *.json > $tmpdir/trait_mapping.out

# loop through *.out files in tmpdir and write contents to corresponding
# file in target_dir
for file in $tmpdir/*.out; do
    echo $file
    while read -r line; do

        # parse out target_file, line# and json line
        # example line: 0201010/variation_archive//updated/000000000000:34094:{...}"
        if [[ "$line" =~ ((.*/00000000000[0-9])\:([0-9]+))\:(.+) ]]; then
            target_file="${target_dir}/${BASH_REMATCH[2]}"

            if [[ ( ! -f $target_file ) ]]; then
                echo "${target_file} is not a valid file"
                echo
                echo "Processing halted and incomplete. Record intergrity compromised."
                exit 1
            else
                #
                key="${BASH_REMATCH[1]//\//\\/}"
                key="${key//:/\\:}"
                echo "${key}"

                if grep -q $key $record_log; then
                    echo "Already exists in file."
                else
                    echo "${BASH_REMATCH[1]}" >> $record_log
                    echo "${BASH_REMATCH[4]}" >> $target_file
                fi
            fi
        else
            echo "ERROR: NO MATCH!!!!!!"
            echo
            echo "Processing halted and incomplete. Record intergrity compromised."
            exit 1
        fi
    done < $file
done

# remove tmpdir
#rm -fR $tmpdir
