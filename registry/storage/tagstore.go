package storage

import (
	"context"
	"os"
	"path"
	"strconv"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
	"golang.org/x/sync/errgroup"
)

var _ distribution.TagService = &TagStore{}

// TagStore provides methods to manage manifest tags in a backend storage driver.
// This implementation uses the same on-disk layout as the (now deleted) tag
// store.  This provides backward compatibility with current registry deployments
// which only makes use of the Digest field of the returned distribution.Descriptor
// but does not enable full roundtripping of Descriptor objects
type TagStore struct {
	repository              *repository
	blobStore               *blobStore
	lookupConcurrencyFactor int
}

func NewStore(ctx context.Context, repository *repository, blobStore *blobStore) *TagStore {
	logger := dcontext.GetLogger(ctx)
	lookupConcurrencyFactor, err := strconv.Atoi(os.Getenv("STORAGE_TAGSTORE_LOOKUP_CONCURRENCY"))
	if err != nil {
		lookupConcurrencyFactor = 64
		logger.Infof("TagStore: STORAGE_TAGSTORE_LOOKUP_CONCURRENCY is not set. Using default %d as lookup concurrency factor", lookupConcurrencyFactor)
	}
	return &TagStore{
		repository:              repository,
		blobStore:               blobStore,
		lookupConcurrencyFactor: lookupConcurrencyFactor,
	}
}

// All returns all tags
func (ts *TagStore) All(ctx context.Context) ([]string, error) {
	var tags []string

	pathSpec, err := pathFor(manifestTagPathSpec{
		name: ts.repository.Named().Name(),
	})
	if err != nil {
		return tags, err
	}

	entries, err := ts.blobStore.driver.List(ctx, pathSpec)
	if err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			return tags, distribution.ErrRepositoryUnknown{Name: ts.repository.Named().Name()}
		default:
			return tags, err
		}
	}

	for _, entry := range entries {
		_, filename := path.Split(entry)
		tags = append(tags, filename)
	}

	return tags, nil
}

// Tag tags the digest with the given tag, updating the the store to point at
// the current tag. The digest must point to a manifest.
func (ts *TagStore) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	currentPath, err := pathFor(manifestTagCurrentPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})

	if err != nil {
		return err
	}

	lbs := ts.linkedBlobStore(ctx, tag)

	// Link into the index
	if err := lbs.linkBlob(ctx, desc); err != nil {
		return err
	}

	// Overwrite the current link
	return ts.blobStore.link(ctx, currentPath, desc.Digest)
}

// resolve the current revision for name and tag.
func (ts *TagStore) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {
	currentPath, err := pathFor(manifestTagCurrentPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})

	if err != nil {
		return distribution.Descriptor{}, err
	}

	revision, err := ts.blobStore.readlink(ctx, currentPath)
	if err != nil {
		switch err.(type) {
		case storagedriver.PathNotFoundError:
			return distribution.Descriptor{}, distribution.ErrTagUnknown{Tag: tag}
		}

		return distribution.Descriptor{}, err
	}

	return distribution.Descriptor{Digest: revision}, nil
}

// Untag removes the tag association
func (ts *TagStore) Untag(ctx context.Context, tag string) error {
	tagPath, err := pathFor(manifestTagPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})
	if err != nil {
		return err
	}

	if err := ts.blobStore.driver.Delete(ctx, tagPath); err != nil {
		switch err.(type) {
		case storagedriver.PathNotFoundError:
			return nil // Untag is idempotent, we don't care if it didn't exist
		default:
			return err
		}
	}

	return nil
}

// linkedBlobStore returns the linkedBlobStore for the named tag, allowing one
// to index manifest blobs by tag name. While the tag store doesn't map
// precisely to the linked blob store, using this ensures the links are
// managed via the same code path.
func (ts *TagStore) linkedBlobStore(ctx context.Context, tag string) *linkedBlobStore {
	return &linkedBlobStore{
		blobStore:  ts.blobStore,
		repository: ts.repository,
		ctx:        ctx,
		linkPathFns: []linkPathFunc{func(name string, dgst digest.Digest) (string, error) {
			return pathFor(manifestTagIndexEntryLinkPathSpec{
				name:     name,
				tag:      tag,
				revision: dgst,
			})

		}},
	}
}

// Lookup recovers a list of tags which refer to this digest.  When a manifest is deleted by
// digest, tag entries which point to it need to be recovered to avoid dangling tags.
func (ts *TagStore) Lookup(ctx context.Context, desc distribution.Descriptor) ([]string, error) {
	allTags, err := ts.All(ctx)
	switch err.(type) {
	case distribution.ErrRepositoryUnknown:
		// This tag store has been initialized but not yet populated
		break
	case nil:
		break
	default:
		return nil, err
	}

	outputChan := make(chan string)

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(ts.lookupConcurrencyFactor)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				close(outputChan)
			}
		}()
		for index := range allTags {
			tag := allTags[index]
			group.Go(func() error {
				tagLinkPathSpec := manifestTagCurrentPathSpec{
					name: ts.repository.Named().Name(),
					tag:  tag,
				}

				tagLinkPath, _ := pathFor(tagLinkPathSpec)
				tagDigest, err := ts.blobStore.readlink(ctx, tagLinkPath)

				if err != nil {
					switch err.(type) {
					// PathNotFoundError shouldn't count as an error
					case storagedriver.PathNotFoundError:
						return nil
					}
					return err
				}

				if tagDigest == desc.Digest {
					outputChan <- tag
				}
				return nil
			})
		}
		group.Wait()
		close(outputChan)
	}()

	var tags []string
	for tag := range outputChan {
		tags = append(tags, tag)
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	return tags, nil
}
